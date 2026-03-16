import pytest

from pathy import Pathy, get_client, use_fs

from . import s3_installed, s3_testable
from .conftest import ENV_ID

S3_ADAPTER = ["s3"]


@pytest.mark.parametrize("adapter", S3_ADAPTER)
@pytest.mark.skipif(not s3_testable, reason="requires s3")
def test_s3_scandir_scandir_continuation_token(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    from pathy.s3 import ScanDirS3

    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/s3_scandir_pagination/")
    for i in range(8):
        (root / f"file{i}.blob").touch()
    client = root.client(root)
    scandir = ScanDirS3(client=client, path=root, prefix=root.prefix, page_size=4)
    blobs = [s.name for s in scandir]
    assert len(blobs) == 8


@pytest.mark.parametrize("adapter", S3_ADAPTER)
@pytest.mark.skipif(not s3_testable, reason="requires s3")
def test_s3_scandir_invalid_bucket_name(with_adapter: str) -> None:
    from pathy.s3 import ScanDirS3

    root = Pathy(f"{with_adapter}://invalid_h3gE_ds5daEf_Sdf15487t2n4/bar")
    client = root.client(root)
    scandir = ScanDirS3(client=client, path=root)
    assert len(list(scandir)) == 0


@pytest.mark.parametrize("adapter", S3_ADAPTER)
@pytest.mark.skipif(not s3_testable, reason="requires s3")
def test_s3_bucket_client_list_blobs(with_adapter: str, bucket: str) -> None:
    """Test corner-case in S3 client that isn't easily reachable from Pathy"""
    from pathy.s3 import BucketClientS3

    client: BucketClientS3 = get_client("s3")
    root = Pathy("s3://invalid_h3gE_ds5daEf_Sdf15487t2n4")
    assert len(list(client.list_blobs(root))) == 0


@pytest.mark.skipif(s3_installed, reason="requires s3 deps to NOT be installed")
def test_s3_import_error_missing_deps() -> None:
    use_fs(False)
    with pytest.raises(ImportError):
        get_client("s3")
