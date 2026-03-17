from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional

try:
    import boto3  # type:ignore
    from botocore.client import ClientError
    from botocore.exceptions import ParamValidationError

    S3NativeClient = Any
    S3NativeBucket = Any
except (ImportError, ModuleNotFoundError):
    raise ImportError(
        """You are using the S3 functionality of Pathy without
    having the required dependencies installed.

    Please try installing them:

        pip install pathy[s3]

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


class BucketEntryS3(BucketEntry):
    bucket: "BucketS3"
    raw: Any


@dataclass
class BlobS3(Blob):
    client: S3NativeClient
    bucket: "BucketS3"

    def delete(self) -> None:
        self.client.delete_object(Bucket=self.bucket.name, Key=self.name)

    def exists(self) -> bool:
        response = self.client.list_objects_v2(
            Bucket=self.bucket.name, Prefix=self.name
        )
        objects = response.get("Contents", [])
        matched = [o["Key"] for o in objects if o["Key"] == self.name]
        return len(matched) > 0


@dataclass
class BucketS3(Bucket):
    name: str
    client: S3NativeClient
    bucket: S3NativeBucket

    def get_blob(self, blob_name: str) -> Optional[BlobS3]:
        blob_stat: Dict[str, Any]
        try:
            blob_stat = self.client.head_object(Bucket=self.name, Key=blob_name)
        except ClientError:
            return None
        updated = blob_stat["LastModified"].timestamp()
        size = blob_stat["ContentLength"]
        return BlobS3(
            client=self.client,
            bucket=self,
            owner=None,  # type:ignore
            name=blob_name,  # type:ignore
            raw=None,
            size=size,
            updated=int(updated),  # type:ignore
        )

    def copy_blob(  # type:ignore[override]
        self, blob: BlobS3, target: "BucketS3", name: str
    ) -> Optional[BlobS3]:
        source = {"Bucket": blob.bucket.name, "Key": blob.name}
        self.client.copy(source, target.name, name)
        pathy_blob: Optional[BlobS3] = self.get_blob(name)
        assert pathy_blob is not None, "copy failed"
        assert pathy_blob.updated is not None, "new blobl has invalid updated time"
        return BlobS3(
            client=self.client,
            bucket=self.bucket,
            owner=None,
            name=name,
            raw=pathy_blob,
            size=pathy_blob.size,
            updated=pathy_blob.updated,
        )

    def delete_blob(self, blob: BlobS3) -> None:  # type:ignore[override]
        self.client.delete_object(Bucket=self.name, Key=blob.name)

    def delete_blobs(self, blobs: List[BlobS3]) -> None:  # type:ignore[override]
        for blob in blobs:
            self.delete_blob(blob)

    def exists(self) -> bool:
        return True


class BucketClientS3(BucketClient):
    client: S3NativeClient
    _session: Optional[boto3.Session]

    @property
    def client_params(self) -> Dict[str, Any]:
        session: Any = self._session
        result: Any = dict() if session is None else dict(client=session.client("s3"))
        return result

    def __init__(self, **kwargs: Any) -> None:
        self.recreate(**kwargs)

    def recreate(self, **kwargs: Any) -> None:
        key_id = kwargs.get("key_id", None)
        key_secret = kwargs.get("key_secret", None)
        boto_session: Any = boto3
        if key_id is not None and key_secret is not None:
            self._session = boto_session = boto3.Session(  # type:ignore
                aws_access_key_id=key_id,
                aws_secret_access_key=key_secret,
            )
        self.client = boto_session.client("s3")  # type:ignore

    def make_uri(self, path: PurePathy) -> str:
        return str(path)

    def create_bucket(  # type:ignore[override]
        self, path: PurePathy
    ) -> S3NativeBucket:
        return self.client.create_bucket(Bucket=path.root)  # type:ignore

    def delete_bucket(self, path: PurePathy) -> None:
        self.client.delete_bucket(Bucket=path.root)

    def exists(self, path: PurePathy) -> bool:
        # Because we want all the parents of a valid blob (e.g. "directory" in
        # "directory/foo.file") to return True, we enumerate the blobs with a prefix
        # and compare the object names to see if they match a substring of the path
        key_name = path.key
        for obj in self.list_blobs(path):
            if obj.name.startswith(key_name + path.pathmod.sep):  # type:ignore
                return True
        return False

    def lookup_bucket(self, path: PurePathy) -> Optional[BucketS3]:
        try:
            return self.get_bucket(path)
        except FileNotFoundError:
            return None

    def get_bucket(self, path: PurePathy) -> BucketS3:
        try:
            native_bucket = self.client.head_bucket(Bucket=path.root)
            return BucketS3(str(path.root), client=self.client, bucket=native_bucket)
        except (ClientError, ParamValidationError):
            raise FileNotFoundError(f"Bucket {path.root} does not exist!")

    def scandir(  # type:ignore[override]
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> PathyScanDir:
        return ScanDirS3(client=self, path=path, prefix=prefix, delimiter=delimiter)

    def list_blobs(
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> Generator[BlobS3, None, None]:
        bucket = self.lookup_bucket(path)
        if bucket is None:
            return
        paginator = self.client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket.name}
        if prefix is not None:
            kwargs["Prefix"] = prefix
        for page in paginator.paginate(**kwargs):
            for item in page.get("Contents", []):
                yield BlobS3(
                    client=self.client,
                    bucket=bucket,
                    owner=None,
                    name=item["Key"],
                    raw=item,
                    size=item["Size"],
                    updated=int(item["LastModified"].timestamp()),
                )


class ScanDirS3(PathyScanDir):
    _client: BucketClientS3

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

    def scandir(self) -> Generator[BucketEntryS3, None, None]:
        sep = self._path.pathmod.sep
        bucket = self._client.lookup_bucket(self._path)
        if bucket is None:
            return

        kwargs: Any = {"Bucket": bucket.name, "Delimiter": sep}
        if self._prefix is not None:
            kwargs["Prefix"] = self._prefix
        if self._page_size is not None:
            kwargs["MaxKeys"] = self._page_size
        continuation_token: Optional[str] = None
        while True:
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self._client.client.list_objects_v2(**kwargs)
            for folder in response.get("CommonPrefixes", []):
                prefix = folder["Prefix"]
                full_name = prefix[:-1] if prefix.endswith(sep) else prefix
                name = full_name.split(sep)[-1]
                yield BucketEntryS3(name, is_dir=True)
            for file in response.get("Contents", ()):
                name = file["Key"].split(sep)[-1]
                yield BucketEntryS3(
                    name=name,
                    is_dir=False,
                    size=file["Size"],
                    last_modified=int(file["LastModified"].timestamp()),
                )
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")


register_client("s3", BucketClientS3)
