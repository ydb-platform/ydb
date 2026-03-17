import datetime
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, cast

try:
    import azure
    from azure.storage.blob import (
        BlobClient,
        BlobProperties,
        BlobServiceClient,
        ContainerClient,
    )
except (ImportError, ModuleNotFoundError):
    raise ImportError(
        """You are using the Azure functionality of Pathy without
    having the required dependencies installed.

    Please try installing them:

        pip install pathy[azure]

    """
    )

from . import (
    Blob,
    Bucket,
    BucketClient,
    BucketEntry,
    PathyScanDir,
    PurePathy,
    register_client,
)


def _safe_last_modified(last_modified: Optional[datetime.datetime]) -> int:
    """Helper to coerce an optional last_modified timestamp into an int"""
    return int(last_modified.timestamp()) if last_modified else 0


class BucketEntryAzure(BucketEntry):
    bucket: "BucketAzure"
    raw: Any


@dataclass
class BlobAzure(Blob):
    client: BlobClient
    bucket: "BucketAzure"

    def delete(self) -> None:
        self.client.delete_blob()

    def exists(self) -> bool:
        return self.client.exists()


@dataclass
class BucketAzure(Bucket):
    name: str
    client: BlobServiceClient
    container: ContainerClient

    def get_blob(self, blob_name: str) -> Optional[BlobAzure]:
        blob_client = self.client.get_blob_client(container=self.name, blob=blob_name)
        if not blob_client.exists():
            return None
        blob_stat = blob_client.get_blob_properties()
        size = blob_stat.size
        return BlobAzure(
            client=blob_client,
            bucket=self,
            owner=None,  # type:ignore
            name=blob_name,  # type:ignore
            raw=None,
            size=size,
            updated=_safe_last_modified(blob_stat.last_modified),
        )

    def copy_blob(  # type:ignore[override]
        self, blob: BlobAzure, target: "BucketAzure", name: str
    ) -> Optional[BlobAzure]:

        copied_blob = self.client.get_blob_client(target.container.container_name, name)
        copied_blob.start_copy_from_url(blob.client.url)
        props = copied_blob.get_blob_properties()
        if props.copy.status != "success" and props.copy.id is not None:
            copied_blob.abort_copy(props.copy.id)
            return None
        pathy_blob = BlobAzure(
            client=copied_blob,
            bucket=target,
            owner=None,
            name=copied_blob.blob_name,
            raw=None,
            size=props.size,
            updated=_safe_last_modified(props.last_modified),
        )
        return pathy_blob

    def delete_blob(self, blob: BlobAzure) -> None:  # type:ignore[override]
        blob.delete()

    def delete_blobs(self, blobs: List[BlobAzure]) -> None:  # type:ignore[override]
        for blob in blobs:
            self.delete_blob(blob)

    def exists(self) -> bool:
        return self.container.exists()


class BucketClientAzure(BucketClient):
    _service: BlobServiceClient

    @property
    def client_params(self) -> Dict[str, Any]:
        return dict(client=self._service)

    def __init__(self, **kwargs: Any) -> None:
        self.recreate(**kwargs)

    def recreate(self, **kwargs: Any) -> None:
        self._service = kwargs.get("service", None)
        if self._service is None:
            connection_string = kwargs.get("connection_string", None)
            if connection_string is None:
                raise ValueError("Expected either 'service' or 'connection_string'")
            self._service = BlobServiceClient.from_connection_string(connection_string)

    def make_uri(self, path: PurePathy) -> str:
        return str(path)

    def create_bucket(  # type:ignore[override]
        self, path: PurePathy
    ) -> ContainerClient:
        container = self._service.get_container_client(container=path.root)
        container.create_container()
        return container

    def delete_bucket(self, path: PurePathy) -> None:
        container = self._service.get_container_client(container=path.root)
        container.delete_container()

    def exists(self, path: PurePathy) -> bool:
        # Because we want all the parents of a valid blob (e.g. "directory" in
        # "directory/foo.file") to return True, we enumerate the blobs with a prefix
        # and compare the object names to see if they match a substring of the path
        key_name = str(path.key)
        for obj in self.list_blobs(path):
            if obj.name.startswith(key_name + path.pathmod.sep):
                return True
        return False

    def lookup_bucket(self, path: PurePathy) -> Optional[BucketAzure]:
        try:
            return self.get_bucket(path)
        except FileNotFoundError:
            return None

    def get_bucket(self, path: PurePathy) -> BucketAzure:
        try:
            native_bucket = self._service.get_container_client(container=path.root)
            if native_bucket.exists():
                return BucketAzure(
                    str(path.root), client=self._service, container=native_bucket
                )
        except azure.core.exceptions.HttpResponseError:  # type: ignore
            pass
        raise FileNotFoundError(f"Bucket {path.root} does not exist!")

    def scandir(  # type:ignore[override]
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> PathyScanDir:
        return ScanDirAzure(client=self, path=path, prefix=prefix, delimiter=delimiter)

    def list_blobs(
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> Generator[BlobAzure, None, None]:
        bucket = self.lookup_bucket(path)
        if bucket is None:
            return
        paginator = bucket.container.list_blobs(name_starts_with=prefix)
        props: BlobProperties
        for props in paginator:
            prop_name = cast(str, props.name)
            blob_client = bucket.container.get_blob_client(prop_name)
            yield BlobAzure(
                client=blob_client,
                bucket=bucket,
                owner=None,
                name=prop_name,
                raw=props,
                size=props.size,
                updated=_safe_last_modified(props.last_modified),
            )


class ScanDirAzure(PathyScanDir):
    _client: BucketClientAzure

    def __init__(
        self,
        client: BucketClient,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> None:
        super().__init__(client=client, path=path, prefix=prefix, delimiter=delimiter)
        self._page_size = page_size

    def scandir(self) -> Generator[BucketEntryAzure, None, None]:
        bucket = self._client.lookup_bucket(self._path)
        if bucket is None:
            return
        paginator = bucket.container.list_blobs(name_starts_with=self._prefix)
        blob: BlobProperties
        seen_dirs: Dict[str, bool] = {}
        for blob in paginator:
            is_dir = False
            blob_name = cast(str, blob.name)
            if self._prefix:
                blob_name = blob_name[len(self._prefix) :]
            if blob_name.find("/") != -1:
                blob_name = blob_name[0 : blob_name.find("/")]
                if blob_name in seen_dirs:
                    continue
                seen_dirs[blob_name] = True
                is_dir = True
            yield BucketEntryAzure(
                name=blob_name,
                is_dir=is_dir,
                size=cast(int, blob.size),
                last_modified=_safe_last_modified(blob.last_modified),
            )


register_client("azure", BucketClientAzure)
