import io
import builtins
import types
import pytest
from cloudfile_mover import core

def test_parse_cloud_url():
    # S3 URL
    provider, bucket, key = core.parse_cloud_url("s3://my-bucket/path/to/object.txt")
    assert provider == "s3"
    assert bucket == "my-bucket"
    assert key == "path/to/object.txt"
    # GCS URL
    provider, bucket, blob = core.parse_cloud_url("gs://mybucket/folder/data.csv")
    assert provider == "gcs"
    assert bucket == "mybucket"
    assert blob == "folder/data.csv"
    # Azure URL with account
    provider, (acct, container), blob = core.parse_cloud_url("azure://myaccount@mycontainer/dir/file.bin")
    assert provider == "azure"
    assert acct == "myaccount"
    assert container == "mycontainer"
    assert blob == "dir/file.bin"
    # Azure URL without account (requires env var in real usage)
    provider, (acct, container), blob = core.parse_cloud_url("azure://mycontainer/dir/file.bin")
    assert provider == "azure"
    assert acct is None
    assert container == "mycontainer"
    assert blob == "dir/file.bin"
    # Azure full URL
    provider, (acct, container), blob = core.parse_cloud_url(
        "https://accountname.blob.core.windows.net/container123/path/data.bin"
    )
    assert provider == "azure"
    assert acct == "accountname"
    assert container == "container123"
    assert blob == "path/data.bin"
    # Unsupported URL
    with pytest.raises(ValueError):
        core.parse_cloud_url("ftp://server/path")

class DummySource(core.S3Source):
    """Inherit from S3Source but override to use in-memory data for testing."""
    def __init__(self, data: bytes):
        # Instead of bucket/key, just store data locally
        self._data = data
        self.size = len(data)
    def read_range(self, offset, length):
        end = min(offset + length, self.size)
        return self._data[offset:end]
    def delete(self):
        # Simulate delete (for test, just mark data as None)
        self._data = None

class DummyDest(core.S3Dest):
    """Inherit from S3Dest but override network calls for testing."""
    def __init__(self):
        # Do not call real S3 create_multipart_upload
        self.parts = []
        self._completed = False
    def upload_part(self, part_number, data):
        # Store the part data internally instead of uploading
        self.parts.append((part_number, data))
    def complete(self):
        # Upon completion, sort parts and combine data to verify integrity
        self.parts.sort(key=lambda x: x[0])
        combined = b"".join(part[1] for part in self.parts)
        self._completed = True
        self.combined_data = combined  # store the final data for verification
    def abort(self):
        # Reset parts list on abort
        self.parts = []

def test_move_file_dummy(monkeypatch):
    # Prepare dummy data and dummy handlers
    data = b"abcdefghijklmnopqrstuvwxyz" * 1000  # 26 KB data
    src = DummySource(data)
    dest = DummyDest()
    # Monkeypatch the parse_cloud_url to return dummy providers 
    monkeypatch.setattr(core, "parse_cloud_url", lambda url: ("s3", "dummy_bucket", "dummy_key") )
    # Monkeypatch instantiation to return our dummy instances
    monkeypatch.setattr(core, "S3Source", lambda bucket, key: src)
    monkeypatch.setattr(core, "S3Dest", lambda bucket, key: dest)
    # Run move_file (it will use our dummy source/dest due to monkeypatch)
    result = core.move_file("s3://dummy_bucket/dummy_key", "s3://dummy_bucket/dummy_key2", threads=4, show_progress=False)
    assert result is True
    # After move, source should be "deleted" (data set to None) and dest should have combined data
    assert src._data is None
    assert dest._completed is True
    # Verify data integrity
    assert hasattr(dest, "combined_data")
    assert dest.combined_data == data
