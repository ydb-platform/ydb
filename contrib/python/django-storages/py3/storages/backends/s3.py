import io
import mimetypes
import os
import posixpath
import tempfile
import threading
import warnings
from datetime import datetime
from datetime import timedelta
from urllib.parse import urlencode

from django.contrib.staticfiles.storage import ManifestFilesMixin
from django.core.exceptions import ImproperlyConfigured
from django.core.exceptions import SuspiciousOperation
from django.core.files.base import File
from django.utils.deconstruct import deconstructible
from django.utils.encoding import filepath_to_uri
from django.utils.timezone import make_naive

from storages.base import BaseStorage
from storages.compress import CompressedFileMixin
from storages.compress import CompressStorageMixin
from storages.utils import ReadBytesWrapper
from storages.utils import check_location
from storages.utils import clean_name
from storages.utils import is_seekable
from storages.utils import lookup_env
from storages.utils import safe_join
from storages.utils import setting
from storages.utils import to_bytes

try:
    import boto3.session
    import botocore
    import s3transfer.constants
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from botocore.signers import CloudFrontSigner
except ImportError as e:
    raise ImproperlyConfigured("Could not load Boto3's S3 bindings. %s" % e)


# NOTE: these are defined as functions so both can be tested
def _use_cryptography_signer():
    # https://cryptography.io as an RSA backend
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    def _cloud_front_signer_from_pem(key_id, pem):
        if isinstance(pem, str):
            pem = pem.encode("ascii")
        key = load_pem_private_key(pem, password=None, backend=default_backend())

        return CloudFrontSigner(
            key_id, lambda x: key.sign(x, padding.PKCS1v15(), hashes.SHA1())
        )

    return _cloud_front_signer_from_pem


def _use_rsa_signer():
    # https://stuvel.eu/rsa as an RSA backend
    import rsa

    def _cloud_front_signer_from_pem(key_id, pem):
        if isinstance(pem, str):
            pem = pem.encode("ascii")
        key = rsa.PrivateKey.load_pkcs1(pem)
        return CloudFrontSigner(key_id, lambda x: rsa.sign(x, key, "SHA-1"))

    return _cloud_front_signer_from_pem


for _signer_factory in (_use_cryptography_signer, _use_rsa_signer):
    try:
        _cloud_front_signer_from_pem = _signer_factory()
        break
    except ImportError:
        pass
else:

    def _cloud_front_signer_from_pem(key_id, pem):
        raise ImproperlyConfigured(
            "An RSA backend is required for signing cloudfront URLs.\n"
            "Supported backends are packages: cryptography and rsa."
        )


def _filter_download_params(params):
    return {
        key: value
        for (key, value) in params.items()
        if key in s3transfer.constants.ALLOWED_DOWNLOAD_ARGS
    }


@deconstructible
class S3File(CompressedFileMixin, File):
    """
    The default file object used by the S3Storage backend.

    This file implements file streaming using boto's multipart
    uploading functionality. The file can be opened in read or
    write mode.

    This class extends Django's File class. However, the contained
    data is only the data contained in the current buffer. So you
    should not access the contained file object directly. You should
    access the data via this class.

    Warning: This file *must* be closed using the close() method in
    order to properly write the file to S3. Be sure to close the file
    in your application.
    """

    def __init__(self, name, mode, storage, buffer_size=None):
        if "r" in mode and "w" in mode:
            raise ValueError("Can't combine 'r' and 'w' in mode.")
        self._storage = storage
        self.name = name[len(self._storage.location) :].lstrip("/")
        self._mode = mode
        self.obj = storage.bucket.Object(name)
        if "w" not in mode:
            # Force early RAII-style exception if object does not exist
            params = _filter_download_params(
                self._storage.get_object_parameters(self.name)
            )
            self.obj.load(**params)
        self._closed = False
        self._file = None
        self._parts = None
        # 5 MB is the minimum part size (if there is more than one part).
        # Amazon allows up to 10,000 parts.  The default supports uploads
        # up to roughly 50 GB.  Increase the part size to accommodate
        # for files larger than this.
        self.buffer_size = buffer_size or setting("AWS_S3_FILE_BUFFER_SIZE", 5242880)
        self._reset_file_properties()

    def _reset_file_properties(self):
        self._multipart = None
        self._raw_bytes_written = 0
        self._write_counter = 0
        self._is_dirty = False

    def open(self, mode=None):
        if self._file is not None and not self.closed:
            self.seek(0)  # Mirror Django's behavior
        elif mode and mode != self._mode:
            raise ValueError("Cannot reopen file with a new mode.")

        # Accessing the file will functionally re-open it
        self.file  # noqa: B018

        return self

    @property
    def size(self):
        return self.obj.content_length

    @property
    def closed(self):
        return self._closed

    def _get_file(self):
        if self._file is None:
            self._file = tempfile.SpooledTemporaryFile(
                max_size=self._storage.max_memory_size,
                suffix=".S3File",
                dir=setting("FILE_UPLOAD_TEMP_DIR"),
            )
            if "r" in self._mode:
                self._is_dirty = False
                params = _filter_download_params(
                    self._storage.get_object_parameters(self.name)
                )
                self.obj.download_fileobj(
                    self._file, ExtraArgs=params, Config=self._storage.transfer_config
                )
                self._file.seek(0)
                if self._storage.gzip and self.obj.content_encoding == "gzip":
                    self._file = self._decompress_file(mode=self._mode, file=self._file)
                elif "b" not in self._mode:
                    if hasattr(self._file, "readable"):
                        # For versions > Python 3.10 compatibility
                        # See SpooledTemporaryFile changes in 3.11 (https://docs.python.org/3/library/tempfile.html) # noqa: E501
                        # Now fully implements the io.BufferedIOBase and io.TextIOBase abstract base classes allowing the file # noqa: E501
                        # to be readable in the mode that it was specified (without accessing the underlying _file object). # noqa: E501
                        # In this case, we need to wrap the file in a TextIOWrapper to ensure that the file is read as a text file. # noqa: E501
                        self._file = io.TextIOWrapper(self._file, encoding="utf-8")
                    else:
                        # For versions <= Python 3.10 compatibility
                        self._file = io.TextIOWrapper(
                            self._file._file, encoding="utf-8"
                        )
            self._closed = False
        return self._file

    def _set_file(self, value):
        self._file = value

    file = property(_get_file, _set_file)

    def read(self, *args, **kwargs):
        if "r" not in self._mode:
            raise AttributeError("File was not opened in read mode.")
        return super().read(*args, **kwargs)

    def readline(self, *args, **kwargs):
        if "r" not in self._mode:
            raise AttributeError("File was not opened in read mode.")
        return super().readline(*args, **kwargs)

    def readlines(self):
        return list(self)

    def write(self, content):
        if "w" not in self._mode:
            raise AttributeError("File was not opened in write mode.")
        self._is_dirty = True
        if self._multipart is None:
            self._multipart = self.obj.initiate_multipart_upload(
                **self._storage._get_write_parameters(self.obj.key)
            )
            self._parts = []
        if self.buffer_size <= self._buffer_file_size:
            self._flush_write_buffer()
        bstr = to_bytes(content)
        self._raw_bytes_written += len(bstr)
        return super().write(bstr)

    @property
    def _buffer_file_size(self):
        pos = self.file.tell()
        self.file.seek(0, os.SEEK_END)
        length = self.file.tell()
        self.file.seek(pos)
        return length

    def _flush_write_buffer(self):
        if self._buffer_file_size:
            self._write_counter += 1
            self.file.seek(0)
            part = self._multipart.Part(self._write_counter)
            response = part.upload(Body=self.file.read())
            self._parts.append(
                {"ETag": response["ETag"], "PartNumber": self._write_counter}
            )
            self.file.seek(0)
            self.file.truncate()

    def _create_empty_on_close(self):
        """
        Attempt to create an empty file for this key when this File is closed if no
        bytes have been written and no object already exists on S3 for this key.

        This behavior is meant to mimic the behavior of Django's builtin
        FileSystemStorage, where files are always created after they are opened in
        write mode:

            f = storage.open('file.txt', mode='w')
            f.close()
        """
        assert "w" in self._mode
        assert self._raw_bytes_written == 0

        try:
            # Check if the object exists on the server; if so, don't do anything
            self.obj.load()
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                self.obj.put(
                    Body=b"", **self._storage._get_write_parameters(self.obj.key)
                )
            else:
                raise

    def close(self):
        if self._is_dirty:
            if self._multipart is not None:
                self._flush_write_buffer()
                self._multipart.complete(MultipartUpload={"Parts": self._parts})
        else:
            if self._multipart is not None:
                self._multipart.abort()
            if "w" in self._mode and self._raw_bytes_written == 0:
                self._create_empty_on_close()
        if self._file is not None:
            self._file.close()
            self._file = None

        self._reset_file_properties()
        self._closed = True


@deconstructible
class S3Storage(CompressStorageMixin, BaseStorage):
    """
    Amazon Simple Storage Service using Boto3

    This storage backend supports opening files in read or write
    mode and supports streaming(buffering) data in chunks to S3
    when writing.
    """

    default_content_type = "application/octet-stream"
    # If config provided in subclass, signature_version and addressing_style
    # settings/args are ignored.
    config = None

    _signers = {}  # noqa: RUF012

    def __init__(self, **settings):
        omitted = object()
        if not hasattr(self, "cloudfront_signer"):
            self.cloudfront_signer = settings.pop("cloudfront_signer", omitted)

        super().__init__(**settings)

        check_location(self)

        if (self.access_key or self.secret_key) and self.session_profile:
            raise ImproperlyConfigured(
                "AWS_S3_SESSION_PROFILE/session_profile should not be provided with "
                "AWS_S3_ACCESS_KEY_ID/access_key and "
                "AWS_S3_SECRET_ACCESS_KEY/secret_key"
            )

        self._bucket = None
        self._connections = threading.local()
        self._unsigned_connections = threading.local()

        if self.config is not None:
            warnings.warn(
                "The 'config' class property is deprecated and will be "
                "removed in a future version. Use AWS_S3_CLIENT_CONFIG "
                "to customize any of the botocore.config.Config parameters.",
                DeprecationWarning,
            )
            self.client_config = self.config

        if self.client_config is None:
            self.client_config = Config(
                s3={"addressing_style": self.addressing_style},
                signature_version=self.signature_version,
                proxies=self.proxies,
            )

        if self.use_threads is False:
            warnings.warn(
                "The AWS_S3_USE_THREADS setting is deprecated. Use "
                "AWS_S3_TRANSFER_CONFIG to customize any of the "
                "boto.s3.transfer.TransferConfig parameters.",
                DeprecationWarning,
            )

        if self.transfer_config is None:
            self.transfer_config = TransferConfig(use_threads=self.use_threads)

        if self.cloudfront_signer is omitted:
            if self.cloudfront_key_id and self.cloudfront_key:
                self.cloudfront_signer = self.get_cloudfront_signer(
                    self.cloudfront_key_id, self.cloudfront_key
                )
            elif bool(self.cloudfront_key_id) ^ bool(self.cloudfront_key):
                raise ImproperlyConfigured(
                    "Both AWS_CLOUDFRONT_KEY_ID/cloudfront_key_id and "
                    "AWS_CLOUDFRONT_KEY/cloudfront_key must be provided together."
                )
            else:
                self.cloudfront_signer = None

    def get_cloudfront_signer(self, key_id, key):
        cache_key = f"{key_id}:{key}"
        if cache_key not in self.__class__._signers:
            self.__class__._signers[cache_key] = _cloud_front_signer_from_pem(
                key_id, key
            )
        return self.__class__._signers[cache_key]

    def get_default_settings(self):
        return {
            "access_key": setting(
                "AWS_S3_ACCESS_KEY_ID",
                setting(
                    "AWS_ACCESS_KEY_ID",
                    lookup_env(["AWS_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"]),
                ),
            ),
            "secret_key": setting(
                "AWS_S3_SECRET_ACCESS_KEY",
                setting(
                    "AWS_SECRET_ACCESS_KEY",
                    lookup_env(["AWS_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"]),
                ),
            ),
            "security_token": setting(
                "AWS_SESSION_TOKEN",
                setting(
                    "AWS_SECURITY_TOKEN",
                    lookup_env(["AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"]),
                ),
            ),
            "session_profile": setting(
                "AWS_S3_SESSION_PROFILE", lookup_env(["AWS_S3_SESSION_PROFILE"])
            ),
            "file_overwrite": setting("AWS_S3_FILE_OVERWRITE", True),
            "object_parameters": setting("AWS_S3_OBJECT_PARAMETERS", {}),
            "bucket_name": setting("AWS_STORAGE_BUCKET_NAME"),
            "querystring_auth": setting("AWS_QUERYSTRING_AUTH", True),
            "querystring_expire": setting("AWS_QUERYSTRING_EXPIRE", 3600),
            "signature_version": setting("AWS_S3_SIGNATURE_VERSION"),
            "location": setting("AWS_LOCATION", ""),
            "custom_domain": setting("AWS_S3_CUSTOM_DOMAIN"),
            "cloudfront_key_id": setting("AWS_CLOUDFRONT_KEY_ID"),
            "cloudfront_key": setting("AWS_CLOUDFRONT_KEY"),
            "addressing_style": setting("AWS_S3_ADDRESSING_STYLE"),
            "file_name_charset": setting("AWS_S3_FILE_NAME_CHARSET", "utf-8"),
            "gzip": setting("AWS_IS_GZIPPED", False),
            "gzip_content_types": setting(
                "GZIP_CONTENT_TYPES",
                (
                    "text/css",
                    "text/javascript",
                    "application/javascript",
                    "application/x-javascript",
                    "image/svg+xml",
                ),
            ),
            "url_protocol": setting("AWS_S3_URL_PROTOCOL", "https:"),
            "endpoint_url": setting("AWS_S3_ENDPOINT_URL"),
            "proxies": setting("AWS_S3_PROXIES"),
            "region_name": setting("AWS_S3_REGION_NAME"),
            "use_ssl": setting("AWS_S3_USE_SSL", True),
            "verify": setting("AWS_S3_VERIFY", None),
            "max_memory_size": setting("AWS_S3_MAX_MEMORY_SIZE", 0),
            "default_acl": setting("AWS_DEFAULT_ACL", None),
            "use_threads": setting("AWS_S3_USE_THREADS", True),
            "transfer_config": setting("AWS_S3_TRANSFER_CONFIG", None),
            "client_config": setting("AWS_S3_CLIENT_CONFIG", None),
        }

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_connections", None)
        state.pop("_unsigned_connections", None)
        state.pop("_bucket", None)
        return state

    def __setstate__(self, state):
        state["_connections"] = threading.local()
        state["_unsigned_connections"] = threading.local()
        state["_bucket"] = None
        self.__dict__ = state

    @property
    def connection(self):
        connection = getattr(self._connections, "connection", None)
        if connection is None:
            session = self._create_session()
            self._connections.connection = session.resource(
                "s3",
                region_name=self.region_name,
                use_ssl=self.use_ssl,
                endpoint_url=self.endpoint_url,
                config=self.client_config,
                verify=self.verify,
            )
        return self._connections.connection

    @property
    def unsigned_connection(self):
        unsigned_connection = getattr(self._unsigned_connections, "connection", None)
        if unsigned_connection is None:
            session = self._create_session()
            config = self.client_config.merge(
                Config(signature_version=botocore.UNSIGNED)
            )
            self._unsigned_connections.connection = session.resource(
                "s3",
                region_name=self.region_name,
                use_ssl=self.use_ssl,
                endpoint_url=self.endpoint_url,
                config=config,
                verify=self.verify,
            )
        return self._unsigned_connections.connection

    def _create_session(self):
        """
        If a user specifies a profile name and this class obtains access keys
        from another source such as environment variables,we want the profile
        name to take precedence.
        """
        if self.session_profile:
            session = boto3.Session(profile_name=self.session_profile)
        else:
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                aws_session_token=self.security_token,
            )
        return session

    @property
    def bucket(self):
        """
        Get the current bucket. If there is no current bucket object
        create it.
        """
        if self._bucket is None:
            self._bucket = self.connection.Bucket(self.bucket_name)
        return self._bucket

    def _normalize_name(self, name):
        """
        Normalizes the name so that paths like /path/to/ignored/../something.txt
        work. We check to make sure that the path pointed to is not outside
        the directory specified by the LOCATION setting.
        """
        try:
            return safe_join(self.location, name)
        except ValueError:
            raise SuspiciousOperation("Attempted access to '%s' denied." % name)

    def _open(self, name, mode="rb"):
        name = self._normalize_name(clean_name(name))
        try:
            f = S3File(name, mode, self)
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                raise FileNotFoundError("File does not exist: %s" % name)
            raise  # Let it bubble up if it was some other error
        return f

    def _save(self, name, content):
        cleaned_name = clean_name(name)
        name = self._normalize_name(cleaned_name)
        params = self._get_write_parameters(name, content)

        if is_seekable(content):
            content.seek(0, os.SEEK_SET)

        # wrap content so read() always returns bytes. This is required for passing it
        # to obj.upload_fileobj() or self._compress_content()
        content = ReadBytesWrapper(content)

        if (
            self.gzip
            and params["ContentType"] in self.gzip_content_types
            and "ContentEncoding" not in params
        ):
            content = self._compress_content(content)
            params["ContentEncoding"] = "gzip"

        obj = self.bucket.Object(name)

        # Workaround file being closed errantly see: https://github.com/boto/s3transfer/issues/80
        original_close = content.close
        content.close = lambda: None
        try:
            obj.upload_fileobj(content, ExtraArgs=params, Config=self.transfer_config)
        finally:
            content.close = original_close
        return cleaned_name

    def delete(self, name):
        try:
            name = self._normalize_name(clean_name(name))
            self.bucket.Object(name).delete()
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                # Not an error to delete something that does not exist
                return

            # Some other error was encountered. Re-raise it
            raise

    def exists(self, name):
        if self.file_overwrite:
            return False

        name = self._normalize_name(clean_name(name))
        try:
            self.connection.meta.client.head_object(Bucket=self.bucket_name, Key=name)
            return True
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False

            # Some other error was encountered. Re-raise it.
            raise

    def listdir(self, name):
        path = self._normalize_name(clean_name(name))
        # The path needs to end with a slash, but if the root is empty, leave it.
        if path and not path.endswith("/"):
            path += "/"

        directories = []
        files = []
        paginator = self.connection.meta.client.get_paginator("list_objects")
        pages = paginator.paginate(Bucket=self.bucket_name, Delimiter="/", Prefix=path)
        for page in pages:
            directories += [
                posixpath.relpath(entry["Prefix"], path)
                for entry in page.get("CommonPrefixes", ())
            ]
            for entry in page.get("Contents", ()):
                key = entry["Key"]
                if key != path:
                    files.append(posixpath.relpath(key, path))
        return directories, files

    def size(self, name):
        name = self._normalize_name(clean_name(name))
        try:
            return self.bucket.Object(name).content_length
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                raise FileNotFoundError("File does not exist: %s" % name)
            raise  # Let it bubble up if it was some other error

    def _get_write_parameters(self, name, content=None):
        params = self.get_object_parameters(name)

        if "ContentType" not in params:
            _type, encoding = mimetypes.guess_type(name)
            content_type = getattr(content, "content_type", None)
            content_type = content_type or _type or self.default_content_type

            params["ContentType"] = content_type
            if encoding:
                params["ContentEncoding"] = encoding

        if "ACL" not in params and self.default_acl:
            params["ACL"] = self.default_acl

        return params

    def get_object_parameters(self, name):
        """
        Returns a dictionary that is passed to file upload. Override this
        method to adjust this on a per-object basis to set e.g ContentDisposition.

        By default, returns the value of AWS_S3_OBJECT_PARAMETERS.

        Setting ContentEncoding will prevent objects from being automatically gzipped.
        """
        return self.object_parameters.copy()

    def get_modified_time(self, name):
        """
        Returns an (aware) datetime object containing the last modified time if
        USE_TZ is True, otherwise returns a naive datetime in the local timezone.
        """
        name = self._normalize_name(clean_name(name))
        entry = self.bucket.Object(name)
        if setting("USE_TZ"):
            # boto3 returns TZ aware timestamps
            return entry.last_modified
        else:
            return make_naive(entry.last_modified)

    def url(self, name, parameters=None, expire=None, http_method=None):
        # Preserve the trailing slash after normalizing the path.
        name = self._normalize_name(clean_name(name))
        params = parameters.copy() if parameters else {}
        if expire is None:
            expire = self.querystring_expire

        if self.custom_domain:
            url = "{}//{}/{}{}".format(
                self.url_protocol,
                self.custom_domain,
                filepath_to_uri(name),
                "?{}".format(urlencode(params)) if params else "",
            )

            if self.querystring_auth and self.cloudfront_signer:
                expiration = datetime.utcnow() + timedelta(seconds=expire)
                return self.cloudfront_signer.generate_presigned_url(
                    url, date_less_than=expiration
                )

            return url

        params["Bucket"] = self.bucket.name
        params["Key"] = name

        connection = (
            self.connection if self.querystring_auth else self.unsigned_connection
        )
        url = connection.meta.client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expire, HttpMethod=http_method
        )
        return url


class S3StaticStorage(S3Storage):
    """Querystring auth must be disabled so that url() returns a consistent output."""

    querystring_auth = False


class S3ManifestStaticStorage(ManifestFilesMixin, S3StaticStorage):
    """Add ManifestFilesMixin with S3StaticStorage."""
