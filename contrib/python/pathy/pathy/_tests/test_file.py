import sys
import pathlib
from typing import Any, Optional

import mock
import pytest

from pathy import BlobFS, BucketClientFS, BucketFS, ClientError, Pathy, get_client

FS_ADAPTER = ["fs"]


def raise_owner(self: Any) -> None:
    raise KeyError("duh")


@pytest.mark.parametrize("adapter", FS_ADAPTER)
@mock.patch.object(pathlib.Path, "owner", raise_owner)
def test_file_get_blob_owner_key_error_protection(with_adapter: str) -> None:
    gs_bucket = Pathy("gs://my_bucket")
    gs_bucket.mkdir()
    path = gs_bucket / "blob.txt"
    path.write_text("hello world!")
    gcs_client: BucketClientFS = get_client("gs")
    bucket: BucketFS = gcs_client.get_bucket(gs_bucket)
    blob: Optional[BlobFS] = bucket.get_blob("blob.txt")
    assert blob is not None and blob.owner is None


@pytest.mark.parametrize("adapter", FS_ADAPTER)
def test_file_bucket_client_fs_open(with_adapter: str) -> None:
    client: BucketClientFS = get_client("gs")
    blob = Pathy("gs://foo/bar")
    with pytest.raises(ClientError):
        client.open(blob)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.touch()
    assert client.open(blob) is not None


@pytest.mark.parametrize("adapter", FS_ADAPTER)
def test_file_bucket_client_fs_make_uri(with_adapter: str) -> None:
    client: BucketClientFS = get_client("gs")
    blob = Pathy("gs://foo/bar")
    actual = client.make_uri(blob)
    if sys.version_info > (3, 13):
        sep = client.root.parser.sep
    else:
        sep = client.root._flavour.sep  # type:ignore
    expected = f"file://{client.root}{sep}foo{sep}bar"
    assert actual == expected

    # Invalid root
    other = Pathy("")
    with pytest.raises(ValueError):
        client.make_uri(other)


@pytest.mark.parametrize("adapter", FS_ADAPTER)
def test_file_bucket_client_fs_create_bucket(with_adapter: str) -> None:
    client: BucketClientFS = get_client("gs")
    # Invalid root
    invalid = Pathy("")
    with pytest.raises(ValueError):
        client.create_bucket(invalid)

    # Can create a bucket with a valid path
    root = Pathy("gs://bucket_name")
    assert client.create_bucket(root) is not None

    # Bucket already exists error
    with pytest.raises(FileExistsError):
        client.create_bucket(root)


@pytest.mark.parametrize("adapter", FS_ADAPTER)
def test_file_bucket_client_fs_get_bucket(with_adapter: str) -> None:
    client: BucketClientFS = get_client("gs")
    # Invalid root
    invalid = Pathy("")
    with pytest.raises(ValueError):
        client.get_bucket(invalid)

    # Can create a bucket with a valid path
    root = Pathy("gs://bucket_name")

    # Bucket doesn't exist error
    with pytest.raises(FileNotFoundError):
        client.get_bucket(root)

    # Create the bucket
    client.create_bucket(root)

    # Now it's found
    assert client.get_bucket(root) is not None


@pytest.mark.parametrize("adapter", FS_ADAPTER)
def test_file_bucket_client_fs_list_blobs(with_adapter: str) -> None:
    client: BucketClientFS = get_client("gs")
    root = Pathy("gs://bucket")
    root.mkdir(exist_ok=True)
    blob = Pathy("gs://bucket/foo/bar/baz")
    blob.touch()
    blobs = [b.name for b in client.list_blobs(root, prefix="foo/bar/")]
    assert blobs == ["foo/bar/baz"]

    blobs = [b.name for b in client.list_blobs(root, prefix="foo/bar/baz")]
    assert len(blobs) == 1
