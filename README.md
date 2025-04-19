# cloudfile-mover

cloudfile-mover is a Python tool and library for transferring large files between cloud storage providers (AWS S3, Google Cloud Storage, Azure Blob Storage). It supports multithreaded transfers, multipart uploads, and secure authentication, making it easier and faster to move big data across clouds.

## Features

- **Cross-cloud transfers:** Move data between S3, GCS, and Azure Blob Storage.
- **High performance:** Uses parallel threads and chunked transfers to maximize throughput.
- **Reliable:** Automatic retries for network issues and robust handling of partial transfers.
- **Progress and logging:** Shows a progress bar by default; optional verbose logging for debugging.
- **True move semantics:** Deletes the source object after a successful transfer.

## Installation

```bash
pip install cloudfile-mover
