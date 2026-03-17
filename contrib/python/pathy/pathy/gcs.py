from dataclasses import dataclass
from typing import Any, Generator, List, Optional

try:
    from google.api_core.exceptions import BadRequest  # type:ignore
    from google.cloud.storage import Blob as GCSNativeBlob  # type:ignore
    from google.cloud.storage import Bucket as GCSNativeBucket  # type:ignore
    from google.cloud.storage import Client as GCSNativeClient  # type:ignore
except (ImportError, ModuleNotFoundError):
    raise ImportError(
        """You are using the GCS functionality of Pathy without
    having the required dependencies installed.

    Please try installing them:

        pip install pathy[gcs]

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


class BucketEntryGCS(BucketEntry):
    bucket: "BucketGCS"
    raw: GCSNativeBlob  # type:ignore[override]


@dataclass
class BlobGCS(Blob):
    def delete(self) -> None:
        self.raw.delete()  # type:ignore

    def exists(self) -> bool:
        return self.raw.exists()  # type:ignore


@dataclass
class BucketGCS(Bucket):
    name: str
    bucket: GCSNativeBucket

    def get_blob(self, blob_name: str) -> Optional[BlobGCS]:
        assert isinstance(
            blob_name, str
        ), f"expected str blob name, but found: {type(blob_name)}"
        native_blob: Optional[Any] = self.bucket.get_blob(blob_name)  # type:ignore
        if native_blob is None:
            return None
        return BlobGCS(
            bucket=self.bucket,
            owner=native_blob.owner,  # type:ignore
            name=native_blob.name,  # type:ignore
            raw=native_blob,
            size=native_blob.size,
            updated=int(native_blob.updated.timestamp()),  # type:ignore
        )

    def copy_blob(  # type:ignore[override]
        self, blob: BlobGCS, target: "BucketGCS", name: str
    ) -> Optional[BlobGCS]:
        assert blob.raw is not None, "raw storage.Blob instance required"
        native_blob: GCSNativeBlob = self.bucket.copy_blob(  # type: ignore
            blob.raw, target.bucket, name
        )
        return BlobGCS(
            bucket=self.bucket,
            owner=native_blob.owner,  # type:ignore
            name=native_blob.name,  # type:ignore
            raw=native_blob,
            size=native_blob.size,
            updated=int(native_blob.updated.timestamp()),  # type:ignore
        )

    def delete_blob(self, blob: BlobGCS) -> None:  # type:ignore[override]
        return self.bucket.delete_blob(blob.name)  # type:ignore

    def delete_blobs(self, blobs: List[BlobGCS]) -> None:  # type:ignore[override]
        return self.bucket.delete_blobs(blobs)  # type:ignore

    def exists(self) -> bool:
        return self.bucket.exists()  # type:ignore


class BucketClientGCS(BucketClient):
    client: GCSNativeClient

    @property
    def client_params(self) -> Any:
        return dict(client=self.client)

    def __init__(self, **kwargs: Any) -> None:
        self.recreate(**kwargs)

    def recreate(self, **kwargs: Any) -> None:
        creds = kwargs["credentials"] if "credentials" in kwargs else None
        if creds is not None:
            kwargs["project"] = creds.project_id
        self.client = GCSNativeClient(**kwargs)

    def make_uri(self, path: PurePathy) -> str:
        return str(path)

    def create_bucket(  # type:ignore[override]
        self, path: PurePathy
    ) -> GCSNativeBucket:
        return self.client.create_bucket(path.root)  # type:ignore

    def delete_bucket(self, path: PurePathy) -> None:
        bucket = self.client.get_bucket(path.root)  # type:ignore
        bucket.delete()  # type:ignore

    def exists(self, path: PurePathy) -> bool:
        # Because we want all the parents of a valid blob (e.g. "directory" in
        # "directory/foo.file") to return True, we enumerate the blobs with a prefix
        # and compare the object names to see if they match a substring of the path
        key_name = path.key
        for obj in self.list_blobs(path):
            if obj.name.startswith(key_name + path.pathmod.sep):
                return True
        return False

    def lookup_bucket(self, path: PurePathy) -> Optional[BucketGCS]:
        try:
            return self.get_bucket(path)
        except FileNotFoundError:
            return None

    def get_bucket(self, path: PurePathy) -> BucketGCS:
        native_bucket: Any = self.client.bucket(path.root)  # type:ignore
        try:
            if native_bucket.exists():
                return BucketGCS(str(path.root), bucket=native_bucket)
        except BadRequest:
            pass
        raise FileNotFoundError(f"Bucket {path.root} does not exist!")

    def scandir(  # type:ignore[override]
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> PathyScanDir:
        return ScanDirGCS(client=self, path=path, prefix=prefix, delimiter=delimiter)

    def list_blobs(
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> Generator[BlobGCS, None, None]:
        bucket = self.lookup_bucket(path)
        if bucket is None:
            return
        response: Any = self.client.list_blobs(  # type:ignore
            path.root, prefix=prefix, delimiter=delimiter
        )
        for page in response.pages:  # type:ignore
            for item in page:
                yield BlobGCS(
                    bucket=bucket,
                    owner=item.owner,
                    name=item.name,
                    raw=item,
                    size=item.size,
                    updated=item.updated.timestamp(),
                )


class ScanDirGCS(PathyScanDir):
    _client: BucketClientGCS

    def scandir(self) -> Generator[BucketEntryGCS, None, None]:
        sep = self._path.pathmod.sep
        bucket = self._client.lookup_bucket(self._path)
        if bucket is None:
            return
        response = self._client.client.list_blobs(  # type:ignore
            bucket.name, prefix=self._prefix, delimiter=sep
        )
        for page in response.pages:  # type:ignore
            folder: str
            for folder in list(page.prefixes):  # type:ignore
                full_name = folder[:-1] if folder.endswith(sep) else folder
                name = full_name.split(sep)[-1]
                if name:
                    yield BucketEntryGCS(name, is_dir=True, raw=None)
            item: Any
            for item in page:  # type:ignore
                name = item.name.split(sep)[-1]
                if name:
                    yield BucketEntryGCS(
                        name=name,
                        is_dir=False,
                        size=item.size,
                        last_modified=item.updated.timestamp(),
                        raw=item,
                    )


register_client("gs", BucketClientGCS)
