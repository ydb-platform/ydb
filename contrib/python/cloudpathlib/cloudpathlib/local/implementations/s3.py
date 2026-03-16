from typing import Any, Optional

from ...cloudpath import CloudImplementation
from ..localclient import LocalClient
from ..localpath import LocalPath


local_s3_implementation = CloudImplementation()
"""Replacement for "s3" CloudImplementation meta object in cloudpathlib.implementation_registry"""


class LocalS3Client(LocalClient):
    """Replacement for S3Client that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    _cloud_meta = local_s3_implementation


LocalS3Client.S3Path = LocalS3Client.CloudPath  # type: ignore


class LocalS3Path(LocalPath):
    """Replacement for S3Path that uses the local file system. Intended as a monkeypatch substitute
    when writing tests.
    """

    cloud_prefix: str = "s3://"
    _cloud_meta = local_s3_implementation

    @property
    def drive(self) -> str:
        return self.bucket

    def mkdir(self, parents=False, exist_ok=False, mode: Optional[Any] = None):
        # not possible to make empty directory on s3
        pass

    @property
    def bucket(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def key(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        # use with boto, etc.
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._md5(self)


LocalS3Path.__name__ = "S3Path"

local_s3_implementation.name = "s3"
local_s3_implementation._client_class = LocalS3Client
local_s3_implementation._path_class = LocalS3Path
