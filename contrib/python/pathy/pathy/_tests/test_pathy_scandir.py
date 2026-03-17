from typing import Generator

import pytest

from pathy import BucketClientFS, BucketEntry, Pathy, PathyScanDir, use_fs


class MockScanDir(PathyScanDir):
    def scandir(self) -> Generator[BucketEntry, None, None]:
        yield BucketEntry("test-bucket", is_dir=True, raw=None)


class AbstractScanDir(PathyScanDir):
    def scandir(self) -> Generator[BucketEntry, None, None]:
        return super().scandir()  # type:ignore


def test_scandir_abstract_methods(bucket: str) -> None:
    use_fs(True)
    client = BucketClientFS()
    root = Pathy(f"gs://{bucket}/")
    with pytest.raises(NotImplementedError):
        AbstractScanDir(client=client, path=root)  # type:ignore


def test_scandir_custom_class(bucket: str) -> None:
    use_fs(True)
    client = BucketClientFS()
    root = Pathy(f"gs://{bucket}/")
    scandir = MockScanDir(client=client, path=root)
    blobs = [b for b in scandir]
    assert len(blobs) == 1


def test_scandir_next(bucket: str) -> None:
    use_fs(True)
    client = BucketClientFS()
    root = Pathy(f"gs://{bucket}/")
    scandir = MockScanDir(client=client, path=root)
    assert next(scandir.__next__()).name == "test-bucket"  # type:ignore
