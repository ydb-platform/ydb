import mimetypes
import os
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

from ..client import Client, register_client_class
from ..cloudpath import implementation_registry
from ..enums import FileCacheMode
from ..exceptions import CloudPathException
from .s3path import S3Path

try:
    from boto3.session import Session
    from boto3.s3.transfer import TransferConfig, S3Transfer
    from botocore.config import Config
    from botocore.exceptions import ClientError
    import botocore.session
except ModuleNotFoundError:
    implementation_registry["s3"].dependencies_loaded = False


@register_client_class("s3")
class S3Client(Client):
    """Client class for AWS S3 which handles authentication with AWS for [`S3Path`](../s3path/)
    instances. See documentation for the [`__init__` method][cloudpathlib.s3.s3client.S3Client.__init__]
    for detailed authentication options."""

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        no_sign_request: Optional[bool] = False,
        botocore_session: Optional["botocore.session.Session"] = None,
        profile_name: Optional[str] = None,
        boto3_session: Optional["Session"] = None,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        endpoint_url: Optional[str] = None,
        boto3_transfer_config: Optional["TransferConfig"] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
        extra_args: Optional[dict] = None,
    ):
        """Class constructor. Sets up a boto3 [`Session`](
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html).
        Directly supports the same authentication interface, as well as the same environment
        variables supported by boto3. See [boto3 Session documentation](
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html).

        If no authentication arguments or environment variables are provided, then the client will
        be instantiated as anonymous, which will only have access to public buckets.

        Args:
            aws_access_key_id (Optional[str]): AWS access key ID.
            aws_secret_access_key (Optional[str]): AWS secret access key.
            aws_session_token (Optional[str]): Session key for your AWS account. This is only
                needed when you are using temporarycredentials.
            no_sign_request (Optional[bool]): If `True`, credentials are not looked for and we use unsigned
                requests to fetch resources. This will only allow access to public resources. This is equivalent
                to `--no-sign-request` in the [AWS CLI](https://docs.aws.amazon.com/cli/latest/reference/).
            botocore_session (Optional[botocore.session.Session]): An already instantiated botocore
                Session.
            profile_name (Optional[str]): Profile name of a profile in a shared credentials file.
            boto3_session (Optional[Session]): An already instantiated boto3 Session.
            file_cache_mode (Optional[Union[str, FileCacheMode]]): How often to clear the file cache; see
                [the caching docs](https://cloudpathlib.drivendata.org/stable/caching/) for more information
                about the options in cloudpathlib.eums.FileCacheMode.
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory. Default can be set with
                the `CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.
            endpoint_url (Optional[str]): S3 server endpoint URL to use for the constructed boto3 S3 resource and client.
                Parameterize it to access a customly deployed S3-compatible object store such as MinIO, Ceph or any other.
            boto3_transfer_config (Optional[dict]): Instantiated TransferConfig for managing
                [s3 transfers](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig)
            content_type_method (Optional[Callable]): Function to call to guess media type (mimetype) when
                writing a file to the cloud. Defaults to `mimetypes.guess_type`. Must return a tuple (content type, content encoding).
            extra_args (Optional[dict]): A dictionary of extra args passed to download, upload, and list functions as relevant. You
                can include any keys supported by upload or download, and we will pass on only the relevant args. To see the extra
                args that are supported look at the upload and download lists in the
                [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer).
        """
        endpoint_url = endpoint_url or os.getenv("AWS_ENDPOINT_URL")
        if boto3_session is not None:
            self.sess = boto3_session
        else:
            self.sess = Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                botocore_session=botocore_session,
                profile_name=profile_name,
            )

        if no_sign_request:
            self.s3 = self.sess.resource(
                "s3",
                endpoint_url=endpoint_url,
                config=Config(signature_version=botocore.session.UNSIGNED),
            )
            self.client = self.sess.client(
                "s3",
                endpoint_url=endpoint_url,
                config=Config(signature_version=botocore.session.UNSIGNED),
            )
        else:
            self.s3 = self.sess.resource("s3", endpoint_url=endpoint_url)
            self.client = self.sess.client("s3", endpoint_url=endpoint_url)

        self.boto3_transfer_config = boto3_transfer_config

        if extra_args is None:
            extra_args = {}

        self._extra_args = extra_args
        self.boto3_dl_extra_args = {
            k: v for k, v in extra_args.items() if k in S3Transfer.ALLOWED_DOWNLOAD_ARGS
        }
        self.boto3_ul_extra_args = {
            k: v for k, v in extra_args.items() if k in S3Transfer.ALLOWED_UPLOAD_ARGS
        }

        # listing ops (list_objects_v2, filter, delete) only accept these extras:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
        self.boto3_list_extra_args = {
            k: self._extra_args[k]
            for k in ["RequestPayer", "ExpectedBucketOwner"]
            if k in self._extra_args
        }
        self._endpoint_url = endpoint_url

        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
        )

    def _get_metadata(self, cloud_path: S3Path) -> Dict[str, Any]:
        # get accepts all download extra args
        data = self.s3.ObjectSummary(cloud_path.bucket, cloud_path.key).get(
            **self.boto3_dl_extra_args
        )

        return {
            "last_modified": data["LastModified"],
            "size": data["ContentLength"],
            "etag": data["ETag"],
            "content_type": data.get("ContentType", None),
            "extra": data["Metadata"],
        }

    def _download_file(self, cloud_path: S3Path, local_path: Union[str, os.PathLike]) -> Path:
        local_path = Path(local_path)
        obj = self.s3.Object(cloud_path.bucket, cloud_path.key)

        obj.download_file(
            str(local_path), Config=self.boto3_transfer_config, ExtraArgs=self.boto3_dl_extra_args
        )
        return local_path

    def _is_file_or_dir(self, cloud_path: S3Path) -> Optional[str]:
        # short-circuit the root-level bucket
        if not cloud_path.key:
            return "dir"

        # get first item by listing at least one key
        return self._s3_file_query(cloud_path)

    def _exists(self, cloud_path: S3Path) -> bool:
        # check if this is a bucket
        if not cloud_path.key:
            extra = {
                k: self._extra_args[k] for k in ["ExpectedBucketOwner"] if k in self._extra_args
            }

            try:
                self.client.head_bucket(Bucket=cloud_path.bucket, **extra)
                return True
            except ClientError:
                return False

        return self._s3_file_query(cloud_path) is not None

    def _s3_file_query(self, cloud_path: S3Path):
        """Boto3 query used for quick checks of existence and if path is file/dir"""
        # check if this is an object that we can access directly
        try:
            # head_object accepts all download extra args (note: Object.load does not accept extra args so we do not use it for this check)
            self.client.head_object(
                Bucket=cloud_path.bucket,
                Key=cloud_path.key.rstrip("/"),
                **self.boto3_dl_extra_args,
            )
            return "file"

        # else, confirm it is a dir by filtering to the first item under the prefix plus a "/"
        except (ClientError, self.client.exceptions.NoSuchKey):
            key = cloud_path.key.rstrip("/") + "/"

            return next(
                (
                    "dir"  # always a dir if we find anything with this query
                    for obj in (
                        self.s3.Bucket(cloud_path.bucket)
                        .objects.filter(Prefix=key, **self.boto3_list_extra_args)
                        .limit(1)
                    )
                ),
                None,
            )

    def _list_dir(self, cloud_path: S3Path, recursive=False) -> Iterable[Tuple[S3Path, bool]]:
        # shortcut if listing all available buckets
        if not cloud_path.bucket:
            if recursive:
                raise NotImplementedError(
                    "Cannot recursively list all buckets and contents; you can get all the buckets then recursively list each separately."
                )

            yield from (
                (self.CloudPath(f"{cloud_path.cloud_prefix}{b['Name']}"), True)
                for b in self.client.list_buckets().get("Buckets", [])
            )
            return

        prefix = cloud_path.key
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        yielded_dirs = set()

        paginator = self.client.get_paginator("list_objects_v2")

        for result in paginator.paginate(
            Bucket=cloud_path.bucket,
            Prefix=prefix,
            Delimiter=("" if recursive else "/"),
            **self.boto3_list_extra_args,
        ):
            # yield everything in common prefixes as directories
            for result_prefix in result.get("CommonPrefixes", []):
                canonical = result_prefix.get("Prefix").rstrip("/")  # keep a canonical form
                if canonical not in yielded_dirs:
                    yield (
                        self.CloudPath(
                            f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{canonical}"
                        ),
                        True,
                    )
                    yielded_dirs.add(canonical)

            # check all the keys
            for result_key in result.get("Contents", []):
                # yield all the parents of any key that have not been yielded already
                o_relative_path = result_key.get("Key")[len(prefix) :]
                for parent in PurePosixPath(o_relative_path).parents:
                    parent_canonical = prefix + str(parent).rstrip("/")
                    if parent_canonical not in yielded_dirs and str(parent) != ".":
                        yield (
                            self.CloudPath(
                                f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{parent_canonical}"
                            ),
                            True,
                        )
                        yielded_dirs.add(parent_canonical)

                # if we already yielded this dir, go to next item in contents
                canonical = result_key.get("Key").rstrip("/")
                if canonical in yielded_dirs:
                    continue

                # s3 fake directories have 0 size and end with "/"
                if result_key.get("Key").endswith("/") and result_key.get("Size") == 0:
                    yield (
                        self.CloudPath(
                            f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{canonical}"
                        ),
                        True,
                    )
                    yielded_dirs.add(canonical)

                # yield object as file
                else:
                    yield (
                        self.CloudPath(
                            f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{result_key.get('Key')}"
                        ),
                        False,
                    )

    def _move_file(self, src: S3Path, dst: S3Path, remove_src: bool = True) -> S3Path:
        # just a touch, so "REPLACE" metadata
        if src == dst:
            o = self.s3.Object(src.bucket, src.key)
            o.copy_from(
                CopySource={"Bucket": src.bucket, "Key": src.key},
                Metadata=self._get_metadata(src).get("extra", {}),
                MetadataDirective="REPLACE",
                **self.boto3_ul_extra_args,
            )

        else:
            target = self.s3.Object(dst.bucket, dst.key)
            target.copy(
                {"Bucket": src.bucket, "Key": src.key},
                ExtraArgs=self.boto3_dl_extra_args,
                Config=self.boto3_transfer_config,
            )

            if remove_src:
                self._remove(src)
        return dst

    def _remove(self, cloud_path: S3Path, missing_ok: bool = True) -> None:
        file_or_dir = self._is_file_or_dir(cloud_path=cloud_path)
        if file_or_dir == "file":
            resp = self.s3.Object(cloud_path.bucket, cloud_path.key).delete(
                **self.boto3_list_extra_args
            )
            if resp.get("ResponseMetadata").get("HTTPStatusCode") not in (204, 200):
                raise CloudPathException(
                    f"Delete operation failed for {cloud_path} with response: {resp}"
                )

        elif file_or_dir == "dir":
            # try to delete as a directory instead
            bucket = self.s3.Bucket(cloud_path.bucket)

            prefix = cloud_path.key
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            resp = bucket.objects.filter(Prefix=prefix, **self.boto3_list_extra_args).delete(
                **self.boto3_list_extra_args
            )
            if resp[0].get("ResponseMetadata").get("HTTPStatusCode") not in (204, 200):
                raise CloudPathException(
                    f"Delete operation failed for {cloud_path} with response: {resp}"
                )

        else:
            if not missing_ok:
                raise FileNotFoundError(
                    f"Cannot delete file that does not exist: {cloud_path} (consider passing missing_ok=True)"
                )

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: S3Path) -> S3Path:
        obj = self.s3.Object(cloud_path.bucket, cloud_path.key)

        extra_args = self.boto3_ul_extra_args.copy()

        if self.content_type_method is not None:
            content_type, content_encoding = self.content_type_method(str(local_path))
            if content_type is not None:
                extra_args["ContentType"] = content_type
            if content_encoding is not None:
                extra_args["ContentEncoding"] = content_encoding

        obj.upload_file(str(local_path), Config=self.boto3_transfer_config, ExtraArgs=extra_args)
        return cloud_path

    def _get_public_url(self, cloud_path: S3Path) -> str:
        """Apparently the best way to get the public URL is to generate a presigned URL
        with the unsigned config set. This creates a temporary unsigned client to generate
        the correct URL
        See: https://stackoverflow.com/a/48197877
        """
        unsigned_config = Config(signature_version=botocore.UNSIGNED)
        unsigned_client = self.sess.client(
            "s3", endpoint_url=self._endpoint_url, config=unsigned_config
        )
        url: str = unsigned_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": cloud_path.bucket, "Key": cloud_path.key},
            ExpiresIn=0,
        )
        return url

    def _generate_presigned_url(self, cloud_path: S3Path, expire_seconds: int = 60 * 60) -> str:
        url: str = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": cloud_path.bucket, "Key": cloud_path.key},
            ExpiresIn=expire_seconds,
        )
        return url


S3Client.S3Path = S3Client.CloudPath  # type: ignore
