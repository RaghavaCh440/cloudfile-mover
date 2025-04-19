# CloudFile-Mover: Cross-Cloud Large File Mover

## Introduction

cloudfile-mover is a Python library designed for high-performance transfer (move) of large files between cloud storage providers: Amazon S3, Google Cloud Storage (GCS), and Azure Blob Storage. It handles the complexities of cross-provider data transfer by efficiently downloading from the source and uploading to the destination in parallel, using multi-threading and cloud-native large file APIs. This tool aims to achieve true "move" semantics – the source file is deleted after a successful copy to the destination – while providing robust features like multipart uploads, progress monitoring, and automatic retries. Moving very large objects across different cloud providers can be challenging. Each provider has its own API and best practices for large file transfers. For example, AWS S3 supports Multipart Upload for files up to 5 TiB (with up to 10,000 parts), requiring each part (except the last) to be at least 5 MiB in size. Google Cloud Storage lacks an exact multipart upload API but allows composing up to 32 separate uploaded objects into one final object. Azure Blob Storage uses Block Blobs, where large files are broken into blocks that can be uploaded independently (even concurrently) and then committed to form the final blob. cloudfile-mover abstracts these details, providing a uniform interface to move data between any two supported cloud buckets.

## Features

**Cross-Provider Transfers**: Move files between S3, GCS, and Azure Blob Storage in any combination (e.g. S3 → GCS, Azure → S3, etc.).
**High Performance via Multithreading**: Utilizes multi-threading to download and upload file chunks in parallel, significantly speeding up transfers for large files.
**Large File Segmentation**: Automatically splits large files into chunks (e.g. using S3 multipart upload, GCS object composition, Azure block blobs) to support very large sizes and improve throughput.
**Robustness with Retries**: Implements retry logic for network hiccups or transient cloud errors on each chunk transfer.
**Progress Bar**: Displays a tqdm progress bar showing the transfer progress (with options to suppress it for non-interactive use).
**Flexible Logging**: Uses Python’s logging to output transfer information. Verbosity can be adjusted (quiet, info, debug) via a CLI flag.
**True Move Semantics**: Upon successful transfer to the destination, the source object is deleted, mimicking a “move” rather than “copy”.
**Secure Authentication**: Leverages standard cloud authentication methods (no credentials are ever hardcoded). Supports AWS IAM roles/keys, Google Application Default Credentials, and Azure DefaultAzureCredential for seamless auth.
**Command-Line Interface (CLI)**: Includes a CLI script cloudfile-mover with options such as --threads, --verbose, --no-progress.
**Importable Library**: Can be used as a Python module with a move_file function for programmatic access.
**PyPI-Ready Package**: Provided with setup files, tests, and documentation for ease of installation and distribution.

## Design and Implementation

## Cross-Provider Transfer Support

To support multiple cloud providers, the library parses URLs with different schemes and dispatches to the appropriate service SDKs:
**AWS S3**: URLs starting with s3://bucket_name/object_path use the boto3 library. The implementation uses S3’s multipart upload API for large files: initiating a multipart upload, uploading each part in parallel, then completing the upload. This approach is necessary to handle big objects (up to 5 TiB) and improves transfer speed by parallelism. We ensure each part meets S3’s size constraints (at least 5 MiB except last)

**Google Cloud Storage**: URLs with gs://bucket_name/blob_path use the google-cloud-storage library. GCS doesn’t have a direct multipart upload, so our strategy is to split the file and upload chunks as temporary objects, then compose them into the final object. GCS allows composing between 1 and 32 objects in a single request (if more than 32 chunks, multiple compose operations are done in sequence). After a successful compose, the temporary chunk objects are deleted to avoid extra storage costs.

**Azure Blob Storage**: URLs with azure://container/blob_path (with an optional account name) use the azure-storage-blob SDK. We utilize Block Blob uploads: the file is divided into blocks, each block is uploaded (staged) independently, and then a commit operation assembles them. This mirrors Azure’s internal approach, where each block upload can happen in parallel. The Azure SDK itself supports parallel uploads via the max_concurrency parameter in upload_blob, but we implement the logic manually to integrate with our cross-cloud streaming design.

During transfers, data is streamed through the running process: each chunk is downloaded from the source and immediately uploaded to the destination. This avoids writing large intermediates to disk. Memory usage is kept in check by processing chunks of a configurable size (by default 64 MiB) and not loading the entire file at once.

## Multithreading and Large File Handling ##

To maximize throughput, cloudfile-mover uses a pool of threads to handle different portions of the file concurrently. The file size is determined via a metadata call (e.g. S3 HeadObject, GCS blob size, Azure get properties) so we know how many chunks to split into. The default chunk size (e.g. 64 MB) and number of threads (e.g. 4 threads) can be adjusted. Each thread is responsible for transferring one chunk of the file:

   **Download**: For each chunk, the source is read from a specific byte range. For S3, we use the Range header in GetObject to fetch a byte range. For GCS, we use the blob’s download_to_file with start and end parameters to read a segment. For Azure, we call download_blob(offset, length) to retrieve a block of bytes. This ensures we only pull the needed segment of data.

   **Upload**: The retrieved chunk is then uploaded to the destination. Depending on the destination:
        **S3**: we call upload_part (with the multipart upload ID) for that chunk. Each uploaded part returns an ETag which we collect for final assembly.
        **GCS**: we upload the chunk to a temporary object (with a unique name suffix). No special API is needed for chunk upload, just the standard upload_from_string for the bytes.
        **Azure**: we call stage_block on the BlobClient with a unique block ID for that chunk. Azure requires block IDs to be base64-encoded; we generate IDs derived from the chunk number (zero-padded) so that we can commit them in order later.

The library takes care to assemble these parts after all threads complete:
        **S3**: calls CompleteMultipartUpload with the list of part ETags and numbers to finalize the object on S3.
        **GCS**: uses the compose operation. We gather all the temporary blob pieces and call destination_blob.compose(sources=...) on them.
 to create the final blob. After a successful compose, the source chunk blobs are deleted (since compose does not remove them automatically).
        **Azure**: calls commit_block_list with the list of block IDs. This finalizes the blob, making all staged blocks part of the committed blob.

**Memory usage**: Each thread holds at most one chunk in memory at a time. For a very large file, if memory is a concern, you can reduce the --threads or chunk size. The default settings are chosen to balance performance with not overwhelming memory or network. 
**Concurrent throughput**: By downloading and uploading in parallel, the transfer can saturate available network bandwidth in both source and destination directions. For example, while one thread is waiting for a chunk download from S3, another thread can be uploading a previously downloaded chunk to Azure. This overlapping of I/O improves total transfer time.

## Retry and Error Handling ##
Network issues or transient cloud API errors can occur, especially for long transfers. cloudfile-mover implements a retry mechanism for each chunk transfer:
Each chunk download/upload operation will be retried a few times (e.g. 3 attempts by default) if an exception is encountered. This covers transient network failures or throttling errors. A brief exponential backoff (increasing sleep between retries) is used to allow the condition to resolve.
If a chunk ultimately fails after retries, the entire transfer is aborted. For S3, an AbortMultipartUpload is issued to ensure partial uploads don’t accumulate and incur storage costs. For GCS, any already uploaded part objects are deleted. For Azure, any staged but uncommitted blocks will expire (and we additionally attempt to delete the blob to discard any partial data). 
An error in any thread will stop the process. The other threads’ futures are cancelled, and the exception is propagated up. This ensures we don’t leave the destination with a partially assembled file. All cleanup is handled before re-raising the error.
By using chunk-level retries, the tool avoids restarting the entire transfer from scratch in case of a minor interruption – only the failed chunk is retried. The final outcome is either a fully successful move (all parts transferred and source deleted) or no change (source remains if move failed).

## Progress Bar and Logging ##
For interactive use, cloudfile-mover provides a live progress bar via tqdm. The progress bar shows the total bytes transferred out of the total file size, updating as each chunk completes. It uses appropriate units (bytes, KB, MB, etc.) and gives an estimated transfer rate and time remaining. This is very useful for tracking large transfers.

If the --no-progress flag is used (or the code is run in a non-TTY environment), the tqdm progress bar is disabled. In such cases, or when running with higher verbosity, logging messages can provide insight into the transfer’s progress (e.g. logging each part completion or any retries).

Logging is done through Python’s standard logging library. By default, only high-level info messages are shown (like start and end of transfer). If the user specifies --verbose (or -v), the logging level is set to DEBUG, which could include per-chunk operations and retry attempts. The logging output is sent to the console.

The CLI supports a --quiet mode implicitly (not printing the progress or any info logs) if the user doesn’t want any output except errors. This is achieved by adjusting log levels or disabling the progress bar accordingly.

## Authentication and Security ##
No sensitive credentials are stored in this library’s code. Instead, it relies on the standard authentication mechanisms provided by each cloud’s SDK:
**AWS S3**: Uses Boto3’s default credential chain. This means it will look for AWS credentials in the environment (AWS_ACCESS_KEY_ID, etc.), AWS config files, or IAM roles attached to the compute (if running on AWS). If running on AWS infrastructure (EC2, ECS, etc.) with an IAM role, no explicit credential is needed – boto3 will automatically use it. If running locally, the user can set environment vars or have a ~/.aws/credentials file.

**Google Cloud Storage**: Uses Google Cloud’s Application Default Credentials (ADC). The google-cloud-storage client will automatically find credentials based on environment, such as a GOOGLE_APPLICATION_CREDENTIALS environment variable (pointing to a service account JSON file) or Google Cloud SDK’s login. ADC is the recommended approach for Google’s client libraries to obtain credentials from the environment or runtime context.

**Azure Blob Storage**: Uses Azure’s DefaultAzureCredential (from azure.identity). This credential is actually a sequence of attempts: it will first check environment variables (like AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET for a service principal), then managed identities (if running on an Azure VM, App Service, etc.), Azure CLI login, and so on. This allows flexible auth without the code needing to know which method is in use. For example, if the user has set environment vars for a service principal with access to the storage account, those will be picked u. If the code is running in Azure with a system-assigned identity, that will be used.

By using these mechanisms, cloudfile-mover avoids ever handling plaintext secrets directly. Users should ensure the environment is configured with appropriate permissions:

The IAM role or keys used for AWS have S3 read permission on the source and write permission on the destination (plus delete permission on the source for the move).

The GCP credentials have Storage Object view/download on the source and Storage Object create/delete on the destination (the compose operation requires storage.objects.create on the bucket and we delete temp objects)

The Azure credentials have Blob read on source, write on destination, and delete on source container.

## Command-Line Interface (CLI) ##
The package installs a console script cloudfile-mover which exposes the functionality via a command-line tool. The CLI usage is:

bash  $ cloudfile-mover SOURCE_URL DEST_URL [--threads N] [--no-progress] [--verbose]
For example:
```$ cloudfile-mover s3://my-bucket/data.bin gs://my-gcs-bucket/data.bin --threads 8 --verbose```
This will transfer data.bin from an S3 bucket to a GCS bucket using 8 threads, with verbose logging. Upon success, the source data.bin on S3 is deleted.

CLI options:
**--threads N (or -t N)**: Number of parallel threads to use (defaults to 4). Using more threads can speed up transfer for high-bandwidth environments, but may consume more memory and network I/O.

**--no-progress**: Disable the tqdm progress bar. Useful for scripting or if output is being captured to a file.

**--verbose (or -v)**: Enable verbose output (DEBUG level logging). This will print details for each chunk and retry, which can help in diagnosing speed bottlenecks or errors.

(Future extension) One could imagine a --quiet flag to suppress even info logs, but by default if you don’t use --verbose, the output is minimal.

The CLI is implemented in the package’s __main__.py so that python -m cloudfile_mover ... also works. It uses Python’s argparse to parse the arguments and then calls the internal move_file function with those parameters.

## Importable Module Usage ##
Developers can also import and use the library directly in Python. For instance:

```from cloudfile_mover import move_file

# Move a blob from Azure to AWS S3, using 5 threads and no progress bar.
move_file("azure://myaccount@sourcecontainer/path/to/blob.dat",
          "s3://target-bucket/path/to/blob.dat",
          threads=5, show_progress=False)
```

The move_file function provides the core functionality. It can be integrated into Python applications, allowing programmatic control (for example, moving multiple files in a loop, or using custom logic to determine source/dest at runtime).
The module interface could also be extended with more granular functions or classes in the future (for example, to support configuring chunk size, or to perform copy without deleting source, etc.), but move_file covers the primary use-case of moving a single object.

## Package Structure ##
The project is organized as a standard Python package, ready to be published to PyPI. The important files and their roles are:

```cloudfile-mover/
├── cloudfile_mover/
│   ├── __init__.py          # Makes the package importable, exposes move_file
│   ├── core.py              # Core logic for transferring files between clouds
│   └── __main__.py          # Entry-point for CLI execution
├── tests/
│   ├── __init__.py
│   └── test_move_file.py    # Unit tests for the library (URL parsing, dummy transfers, etc.)
├── setup.py                 # Setup script for packaging (using setuptools)
├── pyproject.toml           # Build system configuration (PEP 517)
├── README.md                # Documentation and usage instructions
└── LICENSE                  # MIT License text
```
## A few implementation notes regarding  cloudfile_mover/core.py code: ##
We define separate classes for source and destination handling of each provider. This encapsulates provider-specific logic (like how to read a range or upload a part) cleanly.

The move_file function ties everything together: parsing URLs, spawning threads, and cleaning up. We log the start and end of the process, and use debug logs for per-part retries if --verbose is enabled.

The progress bar uses tqdm.update() as chunks complete. We ensure to close the bar (or leave it) after done. In this code, we set leave=False so it doesn’t leave a stale progress line after completion.

In case of failure, we ensure dest.abort() is called to not leave partial data (e.g., aborting S3 multipart so parts are discarded and not billed, deleting any uploaded GCS parts, etc.). The source is not deleted if the move fails.

On success, src.delete() removes the original file. This final step gives the "move" effect.

The function returns True on success, or raises an exception on failure. This can be caught by the CLI to set an exit code if needed.

## cloudfile_mover/__main__.py ##
This __main__.py allows the package to be executed as a script. The entry_points in setup will tie cloudfile-mover command to cloudfile_mover.__main__:main. We parse arguments for source, destination, thread count, verbosity, and progress. The logging.basicConfig here ensures that the logging from our core (which uses the cloudfile_mover logger) will be output according to the chosen level. We then call move_file with appropriate flags. If an exception is raised, we catch it, log an error, and exit with code 1 to indicate failure.

## tests/test_move_file.py ##

In these tests:
We test parse_cloud_url with various inputs to ensure the function correctly parses and returns the expected provider and components.

We create DummySource and DummyDest classes that override the actual network calls. We monkeypatch the core module’s references to S3Source and S3Dest so that when move_file tries to create them, it actually gets our dummy ones. This allows us to simulate a transfer entirely in memory.

The dummy transfer creates a 26KB byte string, moves it, and then we assert that the destination’s combined data matches the original, and the source data is cleared (deleted).

These tests do not require actual cloud credentials and run quickly. In a real test environment, one could also set up integration tests with actual cloud buckets (perhaps using small test files and environment variables for creds), but that would be slower and more complex to automate. The provided dummy test ensures that our threading, chunking, and assembly logic works as expected for correctness.

## setup.py ##
This uses setuptools to define the package metadata and dependencies. The find_packages() will automatically include the cloudfile_mover package. We specify the required third-party libraries. The entry_points creates the command-line tool cloudfile-mover. We also include a few PyPI classifiers for clarity.

## pyproject.toml ##
This minimal pyproject specifies the build system requirements, which allows tools like pip to build the package. We rely on setup.py for metadata, so we don’t duplicate it in pyproject.toml (alternatively, one could use the [project] table in pyproject.toml to declare metadata, but here we keep it simple and traditional).

# Usage #
## Command Line ##
After installation, use the cloudfile-mover command:

```cloudfile-mover s3://source-bucket/large-file.dat gs://target-bucket/large-file.dat --threads 8```

This will transfer large-file.dat from the AWS S3 bucket to the GCS bucket using 8 parallel threads. By default, you'll see a progress bar. Use -v for verbose output or --no-progress to hide the progress bar.

Examples:
**Move from Azure to S3, with verbose logging**:

```cloudfile-mover azure://mycontainer/path/data.bin s3://my-bucket/path/data.bin -t 4 -v```

**Move from GCS to Azure**:

```cloudfile-mover gs://my-gcs-bucket/my.obj azure://mycontainer/my.obj```

**Note on URLs**: For Azure, you can specify the storage account either in the URL (e.g. azure://account@container/blob) or via the environment variable AZURE_STORAGE_ACCOUNT. Ensure your credentials (AWS keys, GCP service account, Azure service principal or managed identity) are set up in the environment so that each SDK can find them.

## Python Library ##
You can also use cloudfile-mover in your Python code:
```from cloudfile_mover import move_file

move_file("s3://my-bucket/data.csv", "azure://myaccount@mycontainer/data.csv", threads=6)
```
This will perform the same operation programmatically. Adjust threads and show_progress as needed. If the move_file call raises no exception, the transfer was successful and the source object has been deleted.

## How it Works ##
cloudfile-mover splits the file into chunks and transfers them in parallel using the cloud providers' APIs:
**AWS S3**: uses multipart upload for files larger than 5MB.
**GCS**: uploads chunks as temporary objects, then uses the compose API to merge them.
**Azure**: uses block blob uploads (staging blocks and then committing them).
This approach enables transferring very large files (multi-GB or even TB) efficiently. See the project documentation for more details on the implementation.

## License ##

```This README provides an overview, installation, usage examples (CLI and code), and a brief mention of how it works and license. In an actual project, one might expand the README with troubleshooting tips or more details on authentication.

### `LICENSE`
```

MIT License Copyright (c) 2025 Raghava Chellu
Permission is hereby granted, free of charge, to any person obtaining a copy ...

```
*(Full MIT License text included here.)*

The MIT license is a permissive license that we include to make the package open-source. Users can refer to this for their rights to use and distribute the code.

---

**Sources:**

The design leverages documented capabilities of each cloud provider for large file transfers:
- AWS S3 Multipart Upload limits and usage&#8203;:contentReference[oaicite:22]{index=22}&#8203;:contentReference[oaicite:23]{index=23}, and the necessity to complete or abort uploads to free storage&#8203;:contentReference[oaicite:24]{index=24}.
- Google Cloud Storage compose operation (max 32 components)&#8203;:contentReference[oaicite:25]{index=25} and need to delete source components after compose&#8203;:contentReference[oaicite:26]{index=26}.
- Azure Block Blob uploads and parallel block staging&#8203;:contentReference[oaicite:27]{index=27}, including Azure SDK’s use of parallel connections for large blobs&#8203;:contentReference[oaicite:28]{index=28}.
- Authentication best practices, such as Google Cloud Application Default Credentials&#8203;:contentReference[oaicite:29]{index=29}.
```
