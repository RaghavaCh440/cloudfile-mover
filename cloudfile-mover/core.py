import os
import re
import math
import logging
import base64
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third-party cloud SDK imports
import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None  # If tqdm is not installed, progress bar will be disabled

# Configure a logger for this module
logger = logging.getLogger("cloudfile_mover")
logger.setLevel(logging.INFO)  # default level (can be overridden by CLI)

def parse_cloud_url(url: str):
    """
    Parse a cloud storage URL and return a tuple indicating provider and identifiers.
    Supported formats:
      - s3://<bucket>/<key>
      - gs://<bucket>/<blob>
      - azure://<container>/<blob>
      - azure://<account>@<container>/<blob>
      - https://<account>.blob.core.windows.net/<container>/<blob>
    """
    # AWS S3
    if url.startswith("s3://"):
        m = re.match(r'^s3://([^/]+)/(.+)$', url)
        if m:
            bucket, key = m.groups()
            return ("s3", bucket, key)
    # Google Cloud Storage
    if url.startswith("gs://"):
        m = re.match(r'^gs://([^/]+)/(.+)$', url)
        if m:
            bucket, blob = m.groups()
            return ("gcs", bucket, blob)
    # Azure (custom scheme)
    if url.startswith("azure://"):
        m = re.match(r'^azure://(?:(\w+)@)?([^/]+)/(.+)$', url)
        if m:
            account, container, blob = m.groups()
            account = account or None
            return ("azure", (account, container), blob)
    # Azure (HTTPS URL format)
    if url.startswith("https://") and ".blob.core.windows.net" in url:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        account = parsed.netloc.split(".")[0]
        path = parsed.path.lstrip('/')
        if '/' not in path:
            raise ValueError("Azure URL missing container/blob information")
        container, blob = path.split('/', 1)
        return ("azure", (account, container), blob)
    raise ValueError(f"Unsupported URL format: {url}")

class S3Source:
    """Source handler for AWS S3."""
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        self.client = boto3.client('s3')  # uses AWS default credentials
        # Get object metadata to retrieve size
        response = self.client.head_object(Bucket=bucket, Key=key)
        self.size = response['ContentLength']
    def get_size(self) -> int:
        return self.size
    def read_range(self, offset: int, length: int) -> bytes:
        # Download a range of bytes [offset, offset+length-1] from the S3 object
        end = offset + length - 1
        range_header = f"bytes={offset}-{end}"
        response = self.client.get_object(Bucket=self.bucket, Key=self.key, Range=range_header)
        data: bytes = response['Body'].read()
        return data
    def delete(self):
        # Delete the source object from S3
        self.client.delete_object(Bucket=self.bucket, Key=self.key)

class GCSSource:
    """Source handler for Google Cloud Storage."""
    def __init__(self, bucket: str, blob_name: str):
        # Uses ADC credentials (e.g., service account or gcloud auth)
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.blob = self.bucket.blob(blob_name)
        # Fetch blob metadata to get size
        self.blob.reload()  # requires Storage Object get permission
        if self.blob.size is None:
            raise FileNotFoundError(f"GCS object gs://{bucket}/{blob_name} not found or size unavailable")
        self.size = self.blob.size
    def get_size(self) -> int:
        return self.size
    def read_range(self, offset: int, length: int) -> bytes:
        # Download a range of bytes [offset, offset+length-1] from the GCS blob
        end = offset + length - 1
        file_obj = bytearray()  # using bytearray as a file-like buffer
        # Google Cloud Storage Python API allows specifying start and end in download_to_file
        self.blob.download_to_file(file_obj, start=offset, end=end)
        data = bytes(file_obj)  # convert bytearray to bytes
        return data
    def delete(self):
        self.blob.delete()  # delete the source blob

class AzureSource:
    """Source handler for Azure Blob Storage."""
    def __init__(self, account: str, container: str, blob_name: str):
        # If account is not provided (None), try environment variable
        if account is None:
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
            if not account:
                raise ValueError("Azure storage account name not provided in URL or AZURE_STORAGE_ACCOUNT env var")
        account_url = f"https://{account}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)
        self.blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
        props = self.blob_client.get_blob_properties()
        self.size = props.size
    def get_size(self) -> int:
        return self.size
    def read_range(self, offset: int, length: int) -> bytes:
        downloader = self.blob_client.download_blob(offset=offset, length=length)
        data: bytes = downloader.readall()
        return data
    def delete(self):
        self.blob_client.delete_blob()

class S3Dest:
    """Destination handler for AWS S3 (multipart upload)."""
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        self.client = boto3.client('s3')
        # Initiate a multipart upload
        resp = self.client.create_multipart_upload(Bucket=bucket, Key=key)
        self.upload_id = resp['UploadId']
        self.parts = []  # to collect ETags and part numbers
    def upload_part(self, part_number: int, data: bytes):
        # Upload a single part to S3
        resp = self.client.upload_part(Bucket=self.bucket, Key=self.key,
                                       UploadId=self.upload_id, PartNumber=part_number,
                                       Body=data)
        etag = resp['ETag']
        # Remove surrounding quotes in ETag if present
        if etag.startswith('"') and etag.endswith('"'):
            etag = etag[1:-1]
        self.parts.append({'ETag': etag, 'PartNumber': part_number})
    def complete(self):
        # Complete the multipart upload to assemble all parts
        # Sort parts by part number to be sure
        self.parts.sort(key=lambda p: p['PartNumber'])
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={'Parts': self.parts}
        )
    def abort(self):
        # Abort the multipart upload (usually on error) to discard parts
        try:
            self.client.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)
        except Exception as e:
            logger.warning(f"S3 abort_multipart_upload failed: {e}")

class GCSDest:
    """Destination handler for Google Cloud Storage (compose from parts)."""
    def __init__(self, bucket: str, blob_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.final_blob_name = blob_name
        # Generate a unique prefix for temporary part objects
        unique = uuid.uuid4().hex  # random hex string
        self.part_prefix = f"{blob_name}.part-{unique}-"
        self.part_count = 0
    def upload_part(self, part_number: int, data: bytes):
        # Upload chunk data as a temporary blob
        part_name = f"{self.part_prefix}{part_number}"
        blob = self.bucket.blob(part_name)
        blob.upload_from_string(data)
        self.part_count += 1
    def complete(self):
        # Compose all part blobs into the final blob
        if self.part_count == 0:
            # If no parts (should not happen for non-zero file), just create empty blob
            self.bucket.blob(self.final_blob_name).upload_from_string(b"")
            return
        # Prepare sources for compose in correct order
        sources = []
        for i in range(1, self.part_count + 1):
            part_name = f"{self.part_prefix}{i}"
            sources.append(self.bucket.blob(part_name))
        dest_blob = self.bucket.blob(self.final_blob_name)
        dest_blob.compose(sources)  # merge the parts into the destination blob&#8203;:contentReference[oaicite:20]{index=20}
        # Clean up temporary part blobs
        for src_blob in sources:
            try:
                src_blob.delete()
            except Exception as e:
                logger.warning(f"Failed to delete temp blob {src_blob.name}: {e}")
    def abort(self):
        # Delete any uploaded part blobs if aborting
        for i in range(1, self.part_count + 1):
            part_name = f"{self.part_prefix}{i}"
            try:
                self.bucket.blob(part_name).delete()
            except Exception:
                pass  # ignore failures on cleanup

class AzureDest:
    """Destination handler for Azure Blob Storage (block blob staging)."""
    def __init__(self, account: str, container: str, blob_name: str):
        if account is None:
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
            if not account:
                raise ValueError("Azure storage account name not provided for destination")
        account_url = f"https://{account}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)
        self.blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
        self.block_ids = []  # list of block IDs (base64 strings)
    def upload_part(self, part_number: int, data: bytes):
        # Generate a block ID. Use zero-padded part number for correct ordering.
        block_id = base64.b64encode(f"{part_number:06d}".encode()).decode('ascii')
        self.blob_client.stage_block(block_id=block_id, data=data)
        self.block_ids.append(block_id)
    def complete(self):
        if not self.block_ids:
            # if no blocks, create an empty blob
            self.blob_client.upload_blob(b"", overwrite=True)
            return
        # Sort block_ids to ensure order (they are zero-padded so sort will be numeric order)
        self.block_ids.sort()
        self.blob_client.commit_block_list(self.block_ids)
    def abort(self):
        # To abort, we can delete the blob to discard any staged blocks (if any were committed or partially uploaded).
        try:
            self.blob_client.delete_blob()
        except Exception:
            pass

def move_file(src_url: str, dst_url: str, threads: int = 4, show_progress: bool = True, verbose: bool = False):
    """
    Move a file from src_url to dst_url.
    Downloads the file from the source cloud storage and uploads it to the destination.
    Uses parallel threads for speed. Deletes the source file upon successful completion.
    """
    # Configure logging level based on verbosity
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # Parse source and destination URLs
    provider_src, bucket_src, key_src = parse_cloud_url(src_url)
    provider_dst, bucket_dst, key_dst = parse_cloud_url(dst_url)
    # Instantiate appropriate source handler
    if provider_src == "s3":
        src = S3Source(bucket_src, key_src)
    elif provider_src == "gcs":
        src = GCSSource(bucket_src, key_src)
    elif provider_src == "azure":
        account, container = bucket_src  # bucket_src is a tuple for Azure
        src = AzureSource(account, container, key_src)
    else:
        raise ValueError(f"Unsupported source provider: {provider_src}")
    # Instantiate appropriate destination handler
    if provider_dst == "s3":
        dest = S3Dest(bucket_dst, key_dst)
    elif provider_dst == "gcs":
        dest = GCSDest(bucket_dst, key_dst)
    elif provider_dst == "azure":
        account, container = bucket_dst  # bucket_dst is tuple for Azure
        dest = AzureDest(account, container, key_dst)
    else:
        raise ValueError(f"Unsupported destination provider: {provider_dst}")
    try:
        file_size = src.get_size()
        logger.info(f"Starting transfer: {src_url} ({file_size} bytes) -> {dst_url}")
        # Determine chunk size and number of parts
        default_chunk = 64 * 1024 * 1024  # 64 MB default chunk size
        if file_size < default_chunk:
            chunk_size = file_size
        else:
            chunk_size = default_chunk
        num_parts = math.ceil(file_size / chunk_size)
        if num_parts < threads:
            threads = num_parts  # no need for more threads than parts
        # Set up progress bar if enabled and tqdm is available
        progress = None
        if show_progress and tqdm is not None:
            progress = tqdm(total=file_size, unit='B', unit_scale=True, desc="Moving", leave=False)
        # Function to copy a single part (to be executed in a thread)
        def transfer_part(part_index: int):
            offset = part_index * chunk_size
            # Last part may be smaller
            bytes_to_read = chunk_size if (offset + chunk_size <= file_size) else (file_size - offset)
            attempt = 0
            while True:
                try:
                    data = src.read_range(offset, bytes_to_read)
                    dest.upload_part(part_index + 1, data)  # part numbers are 1-indexed for APIs
                    if progress:
                        progress.update(len(data))
                    return  # success, break out
                except Exception as e:
                    attempt += 1
                    logger.debug(f"Error transferring part {part_index+1} (attempt {attempt}): {e}")
                    if attempt >= 3:
                        # Give up after 3 attempts
                        raise
                    # small backoff before retrying
                    import time
                    time.sleep(1 * attempt)
        # Launch threads to transfer parts
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(transfer_part, i) for i in range(num_parts)]
            # Wait for all futures to complete, and handle errors
            for future in as_completed(futures):
                # If any future raises, retrieve exception to trigger abort
                future.result()  # this will re-raise any exception that happened
        # All parts transferred, finalize the destination object
        dest.complete()
        # Delete the source object now that transfer is successful
        src.delete()
        logger.info("Transfer completed successfully. Source object deleted.")
        return True
    except Exception as e:
        # On any error, abort the destination upload and propagate the error
        logger.error(f"Transfer failed: {e}")
        try:
            dest.abort()
        except Exception as abort_err:
            logger.error(f"Error during abort/cleanup: {abort_err}")
        # Note: we do not delete the source if transfer failed
        raise
