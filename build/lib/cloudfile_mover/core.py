import os
import re
import math
import logging
import base64
import uuid
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

logger = logging.getLogger("cloudfile_mover")
logger.setLevel(logging.INFO)

def parse_cloud_url(url: str):
    if url.startswith("s3://"):
        m = re.match(r'^s3://([^/]+)/(.+)$', url)
        if m:
            return ("s3", *m.groups())
    if url.startswith("gs://"):
        m = re.match(r'^gs://([^/]+)/(.+)$', url)
        if m:
            return ("gcs", *m.groups())
    if url.startswith("azure://"):
        m = re.match(r'^azure://(?:(\w+)@)?([^/]+)/(.+)$', url)
        if m:
            account, container, blob = m.groups()
            return ("azure", (account or None, container), blob)
    if url.startswith("https://") and ".blob.core.windows.net" in url:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        account = parsed.netloc.split(".")[0]
        container, blob = parsed.path.lstrip('/').split('/', 1)
        return ("azure", (account, container), blob)
    raise ValueError(f"Unsupported URL format: {url}")

# ====================
# Source Handlers
# ====================

class S3Source:
    def __init__(self, bucket: str, key: str):
        self.bucket, self.key = bucket, key
        self.client = boto3.client('s3')
        self.size = self.client.head_object(Bucket=bucket, Key=key)['ContentLength']

    def get_size(self):
        return self.size

    def read_range(self, offset, length):
        end = offset + length - 1
        resp = self.client.get_object(Bucket=self.bucket, Key=self.key, Range=f"bytes={offset}-{end}")
        return resp['Body'].read()

    def delete(self):
        self.client.delete_object(Bucket=self.bucket, Key=self.key)

class GCSSource:
    def __init__(self, bucket: str, blob_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.blob = self.bucket.blob(blob_name)
        self.blob.reload()
        if self.blob.size is None:
            raise FileNotFoundError(f"GCS object gs://{bucket}/{blob_name} not found")
        self.size = self.blob.size

    def get_size(self):
        return self.size

    def read_range(self, offset, length):
        buffer = io.BytesIO()
        self.blob.download_to_file(buffer, start=offset, end=offset+length-1)
        buffer.seek(0)
        return buffer.read()

    def delete(self):
        self.blob.delete()

class AzureSource:
    def __init__(self, account, container, blob_name):
        if account is None:
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
            if not account:
                raise ValueError("Azure storage account not provided")
        service = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=DefaultAzureCredential())
        self.blob_client = service.get_blob_client(container, blob_name)
        self.size = self.blob_client.get_blob_properties().size

    def get_size(self):
        return self.size

    def read_range(self, offset, length):
        return self.blob_client.download_blob(offset=offset, length=length).readall()

    def delete(self):
        self.blob_client.delete_blob()

# ====================
# Destination Handlers
# ====================

class S3Dest:
    def __init__(self, bucket, key):
        self.bucket, self.key = bucket, key
        self.client = boto3.client('s3')
        self.upload_id = self.client.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        self.parts = []

    def upload_part(self, part_number, data):
        resp = self.client.upload_part(Bucket=self.bucket, Key=self.key,
                                       UploadId=self.upload_id, PartNumber=part_number,
                                       Body=data)
        etag = resp['ETag'].strip('"')
        self.parts.append({'ETag': etag, 'PartNumber': part_number})

    def complete(self):
        self.parts.sort(key=lambda p: p['PartNumber'])
        self.client.complete_multipart_upload(Bucket=self.bucket, Key=self.key,
                                              UploadId=self.upload_id,
                                              MultipartUpload={'Parts': self.parts})

    def abort(self):
        self.client.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)

class GCSDest:
    def __init__(self, bucket, blob_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.final_blob_name = blob_name
        self.part_prefix = f"{blob_name}.part-{uuid.uuid4().hex}-"
        self.part_count = 0

    def upload_part(self, part_number, data):
        part_name = f"{self.part_prefix}{part_number}"
        blob = self.bucket.blob(part_name)
        blob.upload_from_file(io.BytesIO(data), size=len(data))
        self.part_count += 1

    def complete(self):
        if self.part_count == 0:
            self.bucket.blob(self.final_blob_name).upload_from_string(b"")
            return
        blobs = [self.bucket.blob(f"{self.part_prefix}{i}") for i in range(1, self.part_count + 1)]
        self.bucket.blob(self.final_blob_name).compose(blobs)
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass

    def abort(self):
        for i in range(1, self.part_count + 1):
            try:
                self.bucket.blob(f"{self.part_prefix}{i}").delete()
            except Exception:
                pass

class AzureDest:
    def __init__(self, account, container, blob_name):
        if account is None:
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
        self.blob_client = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=DefaultAzureCredential()
        ).get_blob_client(container, blob_name)
        self.block_ids = []

    def upload_part(self, part_number, data):
        block_id = base64.b64encode(f"{part_number:06d}".encode()).decode()
        self.blob_client.stage_block(block_id=block_id, data=data)
        self.block_ids.append(block_id)

    def complete(self):
        if not self.block_ids:
            self.blob_client.upload_blob(b"", overwrite=True)
        else:
            self.block_ids.sort()
            self.blob_client.commit_block_list(self.block_ids)

    def abort(self):
        try:
            self.blob_client.delete_blob()
        except Exception:
            pass

# ====================
# Core Transfer Logic
# ====================

def move_file(src_url, dst_url, threads=4, show_progress=True, verbose=False):
    if verbose:
        logger.setLevel(logging.DEBUG)

    provider_src, bucket_src, key_src = parse_cloud_url(src_url)
    provider_dst, bucket_dst, key_dst = parse_cloud_url(dst_url)

    src = {
        "s3": S3Source,
        "gcs": GCSSource,
        "azure": lambda b, k: AzureSource(*b, k)
    }[provider_src](bucket_src, key_src)

    dest = {
        "s3": S3Dest,
        "gcs": GCSDest,
        "azure": lambda b, k: AzureDest(*b, k)
    }[provider_dst](bucket_dst, key_dst)

    file_size = src.get_size()
    logger.info(f"Transferring: {src_url} -> {dst_url} ({file_size} bytes)")

    chunk_size = min(file_size, 64 * 1024 * 1024)
    num_parts = math.ceil(file_size / chunk_size)
    threads = min(threads, num_parts)

    progress = tqdm(total=file_size, unit="B", unit_scale=True, desc="Moving") if show_progress and tqdm else None

    def transfer_part(i):
        offset = i * chunk_size
        length = min(chunk_size, file_size - offset)
        for attempt in range(3):
            try:
                data = src.read_range(offset, length)
                dest.upload_part(i + 1, data)
                if progress:
                    progress.update(len(data))
                return
            except Exception as e:
                logger.warning(f"Retry {i+1}, attempt {attempt+1}: {e}")
                time.sleep(1 + attempt)
        raise RuntimeError(f"Failed to transfer part {i+1}")

    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for future in as_completed([executor.submit(transfer_part, i) for i in range(num_parts)]):
                future.result()
        dest.complete()
        src.delete()
        if progress: progress.close()
        logger.info("Transfer completed successfully.")
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        dest.abort()
        if progress: progress.close()
        raise
