import os
from typing import Any, Optional

from ...cloudpath import CloudImplementation
from ...exceptions import MissingCredentialsError
from ..localclient import LocalClient
from ..localpath import LocalPath


local_azure_blob_implementation = CloudImplementation()
"""Replacement for "azure" CloudImplementation meta object in
cloudpathlib.implementation_registry"""


class LocalAzureBlobClient(LocalClient):
    """Replacement for AzureBlobClient that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    _cloud_meta = local_azure_blob_implementation

    def __init__(self, *args, **kwargs):
        cred_opts = [
            kwargs.get("blob_service_client", None),
            kwargs.get("connection_string", None),
            kwargs.get("account_url", None),
            os.getenv("AZURE_STORAGE_CONNECTION_STRING", None),
        ]
        super().__init__(*args, **kwargs)

        if all(opt is None for opt in cred_opts):
            raise MissingCredentialsError(
                "AzureBlobClient does not support anonymous instantiation. "
                "Credentials are required; see docs for options."
            )


LocalAzureBlobClient.AzureBlobPath = LocalAzureBlobClient.CloudPath  # type: ignore


class LocalAzureBlobPath(LocalPath):
    """Replacement for AzureBlobPath that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    cloud_prefix: str = "az://"
    _cloud_meta = local_azure_blob_implementation

    @property
    def drive(self) -> str:
        return self.container

    def mkdir(self, parents=False, exist_ok=False, mode: Optional[Any] = None):
        # not possible to make empty directory on blob storage
        pass

    @property
    def container(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def blob(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._md5(self)

    @property
    def md5(self) -> str:
        return self.client._md5(self)


LocalAzureBlobPath.__name__ = "AzureBlobPath"

local_azure_blob_implementation.name = "azure"
local_azure_blob_implementation._client_class = LocalAzureBlobClient
local_azure_blob_implementation._path_class = LocalAzureBlobPath
