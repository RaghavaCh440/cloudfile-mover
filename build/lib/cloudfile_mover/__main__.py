import argparse
import logging
from .core import move_file

def main():
    parser = argparse.ArgumentParser(prog="cloudfile-mover",
        description="Move large files between AWS S3, Google Cloud Storage, and Azure Blob Storage.")
    parser.add_argument("source", help="Source file URL (s3://, gs://, or azure://)")
    parser.add_argument("destination", help="Destination file URL (s3://, gs://, or azure://)")
    parser.add_argument("-t", "--threads", type=int, default=4, help="Number of threads for parallel transfer")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging output")
    args = parser.parse_args()
    # Configure logging to console
    logging.basicConfig(format="%(message)s", level=logging.DEBUG if args.verbose else logging.INFO)
    try:
        move_file(args.source, args.destination, threads=args.threads, 
                  show_progress=not args.no_progress, verbose=args.verbose)
    except Exception as e:
        logging.error(f"Failed to move file: {e}")
        exit(1)

if __name__ == "__main__":
    main()
