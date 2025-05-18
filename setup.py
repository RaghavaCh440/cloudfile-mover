from setuptools import setup, find_packages

setup(
    name="cloudfile-mover",
    version="0.1.0",
    description="Move large files between AWS S3, Google Cloud Storage, and Azure Blob Storage with high performance.",
    author="Raghava Chellu",
    author_email="you@example.com",
    url="https://github.com/youruser/cloudfile-mover",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "boto3",
        "google-cloud-storage",
        "azure-storage-blob",
        "azure-identity",
        "tqdm"
    ],
    entry_points={
        "console_scripts": [
            "cloudfile-mover = cloudfile_mover.__main__:main"
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Internet :: File Transfer Protocol (FTP)",  # not exactly FTP, but related to file transfer
        "Intended Audience :: Developers"
    ]
)
