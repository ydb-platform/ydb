from datetime import datetime, timedelta
import mimetypes
import os
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union
from itertools import islice

try:
    from typing import cast
except ImportError:
    from typing_extensions import cast

from ..client import Client, register_client_class
from ..cloudpath import implementation_registry
from ..enums import FileCacheMode
from ..exceptions import MissingCredentialsError
from .azblobpath import AzureBlobPath


try:
    from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
    from azure.core.credentials import AzureNamedKeyCredential

    from azure.storage.blob import (
        BlobPrefix,
        BlobSasPermissions,
        BlobServiceClient,
        BlobProperties,
        ContentSettings,
        generate_blob_sas,
    )

    from azure.storage.blob._shared.authentication import (
        SharedKeyCredentialPolicy as BlobSharedKeyCredentialPolicy,
    )

    from azure.storage.filedatalake import DataLakeServiceClient, FileProperties
    from azure.storage.filedatalake._shared.authentication import (
        SharedKeyCredentialPolicy as DataLakeSharedKeyCredentialPolicy,
    )

except ModuleNotFoundError:
    implementation_registry["azure"].dependencies_loaded = False


@register_client_class("azure")
class AzureBlobClient(Client):
    """Client class for Azure Blob Storage which handles authentication with Azure for
    [`AzureBlobPath`](../azblobpath/) instances. See documentation for the
    [`__init__` method][cloudpathlib.azure.azblobclient.AzureBlobClient.__init__] for detailed
    authentication options.
    """

    def __init__(
        self,
        account_url: Optional[str] = None,
        credential: Optional[Any] = None,
        connection_string: Optional[str] = None,
        blob_service_client: Optional["BlobServiceClient"] = None,
        data_lake_client: Optional["DataLakeServiceClient"] = None,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
    ):
        """Class constructor. Sets up a [`BlobServiceClient`](
        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).
        Supports the following authentication methods of `BlobServiceClient`.

        - Environment variable `""AZURE_STORAGE_CONNECTION_STRING"` containing connecting string
        with account credentials. See [Azure Storage SDK documentation](
        https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal).
        - Connection string via `connection_string`, authenticated either with an embedded SAS
        token or with credentials passed to `credentials`.
        - Account URL via `account_url`, authenticated either with an embedded SAS token, or with
        credentials passed to `credentials`.
        - Instantiated and already authenticated [`BlobServiceClient`](
        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python) or
        [`DataLakeServiceClient`](https://learn.microsoft.com/en-us/python/api/azure-storage-file-datalake/azure.storage.filedatalake.datalakeserviceclient).

        If multiple methods are used, priority order is reverse of list above (later in list takes
        priority). If no methods are used, a [`MissingCredentialsError`][cloudpathlib.exceptions.MissingCredentialsError]
        exception will be raised raised.

        Args:
            account_url (Optional[str]): The URL to the blob storage account, optionally
                authenticated with a SAS token. See documentation for [`BlobServiceClient`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).
            credential (Optional[Any]): Credentials with which to authenticate. Can be used with
                `account_url` or `connection_string`, but is unnecessary if the other already has
                an SAS token. See documentation for [`BlobServiceClient`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python)
                or [`BlobServiceClient.from_connection_string`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python#from-connection-string-conn-str--credential-none----kwargs-).
            connection_string (Optional[str]): A connection string to an Azure Storage account. See
                [Azure Storage SDK documentation](
                https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal).
            blob_service_client (Optional[BlobServiceClient]): Instantiated [`BlobServiceClient`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).
            data_lake_client (Optional[DataLakeServiceClient]): Instantiated [`DataLakeServiceClient`](
                https://learn.microsoft.com/en-us/python/api/azure-storage-file-datalake/azure.storage.filedatalake.datalakeserviceclient).
                If None and `blob_service_client` is passed, we will create based on that.
                Otherwise, will create based on passed credential, account_url, connection_string, or AZURE_STORAGE_CONNECTION_STRING env var
            file_cache_mode (Optional[Union[str, FileCacheMode]]): How often to clear the file cache; see
                [the caching docs](https://cloudpathlib.drivendata.org/stable/caching/) for more information
                about the options in cloudpathlib.eums.FileCacheMode.
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory. Default can be set with
                the `CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.
            content_type_method (Optional[Callable]): Function to call to guess media type (mimetype) when
                writing a file to the cloud. Defaults to `mimetypes.guess_type`. Must return a tuple (content type, content encoding).
        """
        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
        )

        if connection_string is None:
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", None)

        self.data_lake_client: Optional[DataLakeServiceClient] = (
            None  # only needs to end up being set if HNS is enabled
        )

        if blob_service_client is not None:
            self.service_client = blob_service_client

            # create from blob service client if not passed
            if data_lake_client is None:
                credential = (
                    blob_service_client.credential
                    if not isinstance(
                        blob_service_client.credential, BlobSharedKeyCredentialPolicy
                    )
                    else AzureNamedKeyCredential(
                        blob_service_client.credential.account_name,
                        blob_service_client.credential.account_key,
                    )
                )

                self.data_lake_client = DataLakeServiceClient(
                    account_url=self.service_client.url.replace(".blob.", ".dfs.", 1),
                    credential=credential,
                )
            else:
                self.data_lake_client = data_lake_client

        elif data_lake_client is not None:
            self.data_lake_client = data_lake_client

            if blob_service_client is None:

                credential = (
                    data_lake_client.credential
                    if not isinstance(
                        data_lake_client.credential, DataLakeSharedKeyCredentialPolicy
                    )
                    else AzureNamedKeyCredential(
                        data_lake_client.credential.account_name,
                        data_lake_client.credential.account_key,
                    )
                )

                self.service_client = BlobServiceClient(
                    account_url=self.data_lake_client.url.replace(".dfs.", ".blob.", 1),
                    credential=credential,
                )

        elif connection_string is not None:
            self.service_client = BlobServiceClient.from_connection_string(
                conn_str=connection_string, credential=credential
            )
            self.data_lake_client = DataLakeServiceClient.from_connection_string(
                conn_str=connection_string, credential=credential
            )
        elif account_url is not None:
            if ".dfs." in account_url:
                self.service_client = BlobServiceClient(
                    account_url=account_url.replace(".dfs.", ".blob."), credential=credential
                )
                self.data_lake_client = DataLakeServiceClient(
                    account_url=account_url, credential=credential
                )
            elif ".blob." in account_url:
                self.service_client = BlobServiceClient(
                    account_url=account_url, credential=credential
                )
                self.data_lake_client = DataLakeServiceClient(
                    account_url=account_url.replace(".blob.", ".dfs."), credential=credential
                )
            else:
                # assume default to blob; HNS not supported
                self.service_client = BlobServiceClient(
                    account_url=account_url, credential=credential
                )

        else:
            raise MissingCredentialsError(
                "AzureBlobClient does not support anonymous instantiation. "
                "Credentials are required; see docs for options."
            )

        self._hns_enabled: Optional[bool] = None

    def _check_hns(self, cloud_path: AzureBlobPath) -> Optional[bool]:
        if self._hns_enabled is None:
            try:
                account_info = self.service_client.get_account_information()  # type: ignore
                self._hns_enabled = account_info.get("is_hns_enabled", False)  # type: ignore

            # get_account_information() not supported with this credential; we have to fallback to
            # checking if the root directory exists and is a has 'metadata': {'hdi_isfolder': 'true'}
            except ResourceNotFoundError:
                return self._check_hns_root_metadata(cloud_path)
            except HttpResponseError as error:
                if error.status_code == HTTPStatus.FORBIDDEN:
                    return self._check_hns_root_metadata(cloud_path)
                else:
                    raise

        return self._hns_enabled

    def _check_hns_root_metadata(self, cloud_path: AzureBlobPath) -> bool:
        root_dir = self.service_client.get_blob_client(container=cloud_path.container, blob="/")

        self._hns_enabled = (
            root_dir.exists()
            and root_dir.get_blob_properties().metadata.get("hdi_isfolder", False) == "true"
        )

        return cast(bool, self._hns_enabled)

    def _get_metadata(
        self, cloud_path: AzureBlobPath
    ) -> Union["BlobProperties", "FileProperties", Dict[str, Any]]:
        if self._check_hns(cloud_path):

            # works on both files and directories
            fsc = self.data_lake_client.get_file_system_client(cloud_path.container)  # type: ignore

            if fsc is not None:
                properties = fsc.get_file_client(cloud_path.blob).get_file_properties()

            # no content settings on directory
            properties["content_type"] = properties.get(
                "content_settings", {"content_type": None}
            ).get("content_type")

        else:
            blob = self.service_client.get_blob_client(
                container=cloud_path.container, blob=cloud_path.blob
            )
            properties = blob.get_blob_properties()

            properties["content_type"] = properties.content_settings.content_type

        return properties

    @staticmethod
    def _partial_filename(local_path) -> Path:
        return Path(str(local_path) + ".part")

    def _download_file(
        self, cloud_path: AzureBlobPath, local_path: Union[str, os.PathLike]
    ) -> Path:
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )

        download_stream = blob.download_blob()

        local_path = Path(local_path)

        local_path.parent.mkdir(exist_ok=True, parents=True)

        try:
            partial_local_path = self._partial_filename(local_path)
            with partial_local_path.open("wb") as data:
                download_stream.readinto(data)

            partial_local_path.replace(local_path)
        except:  # noqa: E722
            # remove any partial download
            if partial_local_path.exists():
                partial_local_path.unlink()
            raise

        return local_path

    def _is_file_or_dir(self, cloud_path: AzureBlobPath) -> Optional[str]:
        # short-circuit the root-level container
        if not cloud_path.blob:
            return "dir"

        try:
            meta = self._get_metadata(cloud_path)

            # if hns, has is_directory property; else if not hns, _get_metadata will raise if not a file
            return (
                "dir"
                if meta.get("is_directory", False)
                or meta.get("metadata", {}).get("hdi_isfolder", False)
                else "file"
            )

        # thrown if not HNS and file does not exist _or_ is dir; check if is dir instead
        except ResourceNotFoundError:
            prefix = cloud_path.blob
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # not a file, see if it is a directory
            container_client = self.service_client.get_container_client(cloud_path.container)

            try:
                next(container_client.list_blobs(name_starts_with=prefix))
                return "dir"
            except StopIteration:
                return None

    def _exists(self, cloud_path: AzureBlobPath) -> bool:
        # short circuit when only the container
        if not cloud_path.blob:
            return self.service_client.get_container_client(cloud_path.container).exists()

        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(
        self, cloud_path: AzureBlobPath, recursive: bool = False
    ) -> Iterable[Tuple[AzureBlobPath, bool]]:
        if not cloud_path.container:
            for container in self.service_client.list_containers():
                yield self.CloudPath(f"{cloud_path.cloud_prefix}{container.name}"), True

                if not recursive:
                    continue

                yield from self._list_dir(
                    self.CloudPath(f"{cloud_path.cloud_prefix}{container.name}"), recursive=True
                )
            return

        container_client = self.service_client.get_container_client(cloud_path.container)

        prefix = cloud_path.blob
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        if self._check_hns(cloud_path):
            file_system_client = self.data_lake_client.get_file_system_client(cloud_path.container)  # type: ignore
            paths = file_system_client.get_paths(path=cloud_path.blob, recursive=recursive)

            for path in paths:
                yield self.CloudPath(
                    f"{cloud_path.cloud_prefix}{cloud_path.container}/{path.name}"
                ), path.is_directory

        else:
            if not recursive:
                blobs = container_client.walk_blobs(name_starts_with=prefix)  # type: ignore
            else:
                blobs = container_client.list_blobs(name_starts_with=prefix)  # type: ignore

            for blob in blobs:
                # walk_blobs returns folders with a trailing slash
                blob_path = blob.name.rstrip("/")
                blob_cloud_path = self.CloudPath(
                    f"{cloud_path.cloud_prefix}{cloud_path.container}/{blob_path}"
                )

                yield blob_cloud_path, (
                    isinstance(blob, BlobPrefix)
                    if not recursive
                    else False  # no folders from list_blobs in non-hns storage accounts
                )

    def _move_file(
        self, src: AzureBlobPath, dst: AzureBlobPath, remove_src: bool = True
    ) -> AzureBlobPath:
        # just a touch, so "REPLACE" metadata
        if src == dst:
            blob_client = self.service_client.get_blob_client(
                container=src.container, blob=src.blob
            )

            blob_client.set_blob_metadata(
                metadata=dict(last_modified=str(datetime.utcnow().timestamp()))
            )

        # we can use rename API when the same account on adls gen2
        elif remove_src and (src.client is dst.client) and self._check_hns(src):
            fsc = self.data_lake_client.get_file_system_client(src.container)  # type: ignore

            if src.is_dir():
                fsc.get_directory_client(src.blob).rename_directory(f"{dst.container}/{dst.blob}")
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                fsc.get_file_client(src.blob).rename_file(f"{dst.container}/{dst.blob}")

        else:
            target = self.service_client.get_blob_client(container=dst.container, blob=dst.blob)

            source = self.service_client.get_blob_client(container=src.container, blob=src.blob)

            target.start_copy_from_url(source.url)

            if remove_src:
                self._remove(src)

        return dst

    def _mkdir(
        self, cloud_path: AzureBlobPath, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if self._check_hns(cloud_path):
            file_system_client = self.data_lake_client.get_file_system_client(cloud_path.container)  # type: ignore
            directory_client = file_system_client.get_directory_client(cloud_path.blob)

            if not exist_ok and directory_client.exists():
                raise FileExistsError(f"Directory already exists: {cloud_path}")

            if not parents:
                if not self._exists(cloud_path.parent):
                    raise FileNotFoundError(
                        f"Parent directory does not exist ({cloud_path.parent}). To create parent directories, use `parents=True`."
                    )

            directory_client.create_directory()
        else:
            # consistent with other mkdir no-op behavior on other backends if not supported
            pass

    def _remove(self, cloud_path: AzureBlobPath, missing_ok: bool = True) -> None:
        file_or_dir = self._is_file_or_dir(cloud_path)
        if file_or_dir == "dir":
            if self._check_hns(cloud_path):
                _hns_rmtree(self.data_lake_client, cloud_path.container, cloud_path.blob)
                return

            blobs = (
                b.blob for b, is_dir in self._list_dir(cloud_path, recursive=True) if not is_dir
            )
            container_client = self.service_client.get_container_client(cloud_path.container)
            while batch := tuple(islice(blobs, 256)):
                container_client.delete_blobs(*batch)
        elif file_or_dir == "file":
            blob = self.service_client.get_blob_client(
                container=cloud_path.container, blob=cloud_path.blob
            )

            blob.delete_blob()
        else:
            # Does not exist
            if not missing_ok:
                raise FileNotFoundError(f"File does not exist: {cloud_path}")

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: AzureBlobPath
    ) -> AzureBlobPath:
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )

        extra_args = {}
        if self.content_type_method is not None:
            content_type, content_encoding = self.content_type_method(str(local_path))

            if content_type is not None:
                extra_args["content_type"] = content_type
            if content_encoding is not None:
                extra_args["content_encoding"] = content_encoding

        content_settings = ContentSettings(**extra_args)

        with Path(local_path).open("rb") as data:
            blob.upload_blob(data, overwrite=True, content_settings=content_settings)  # type: ignore

        return cloud_path

    def _get_public_url(self, cloud_path: AzureBlobPath) -> str:
        blob_client = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )
        return blob_client.url

    def _generate_presigned_url(
        self, cloud_path: AzureBlobPath, expire_seconds: int = 60 * 60
    ) -> str:
        sas_token = generate_blob_sas(
            self.service_client.account_name,  # type: ignore[arg-type]
            container_name=cloud_path.container,
            blob_name=cloud_path.blob,
            account_key=self.service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(seconds=expire_seconds),
        )
        url = f"{self._get_public_url(cloud_path)}?{sas_token}"
        return url


def _hns_rmtree(data_lake_client, container, directory):
    """Stateless implementation so can be used in test suite cleanup as well.

    If hierarchical namespace is enabled, delete the directory and all its contents.
    (The non-HNS version is implemented in `_remove`, but will leave empty folders in HNS).
    """
    file_system_client = data_lake_client.get_file_system_client(container)
    directory_client = file_system_client.get_directory_client(directory)
    directory_client.delete_directory()


AzureBlobClient.AzureBlobPath = AzureBlobClient.CloudPath  # type: ignore
