from typing import Any, Optional

from ...cloudpath import CloudImplementation
from ..localclient import LocalClient
from ..localpath import LocalPath


local_gs_implementation = CloudImplementation()
"""Replacement for "gs" CloudImplementation meta object in cloudpathlib.implementation_registry"""


class LocalGSClient(LocalClient):
    """Replacement for GSClient that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    _cloud_meta = local_gs_implementation


LocalGSClient.GSPath = LocalGSClient.CloudPath  # type: ignore


class LocalGSPath(LocalPath):
    """Replacement for GSPath that uses the local file system. Intended as a monkeypatch substitute
    when writing tests.
    """

    cloud_prefix: str = "gs://"
    _cloud_meta = local_gs_implementation

    @property
    def drive(self) -> str:
        return self.bucket

    def mkdir(self, parents=False, exist_ok=False, mode: Optional[Any] = None):
        # not possible to make empty directory on gs
        pass

    @property
    def bucket(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def blob(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        # use with boto, etc.
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._md5(self)

    @property
    def md5(self) -> str:
        return self.client._md5(self)


LocalGSPath.__name__ = "GSPath"

local_gs_implementation.name = "gs"
local_gs_implementation._client_class = LocalGSClient
local_gs_implementation._path_class = LocalGSPath
