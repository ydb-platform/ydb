import gzip
import io
import mimetypes
from datetime import timedelta
from tempfile import SpooledTemporaryFile

from django.core.exceptions import ImproperlyConfigured
from django.core.exceptions import SuspiciousOperation
from django.core.files.base import File
from django.utils import timezone
from django.utils.deconstruct import deconstructible

from storages.base import BaseStorage
from storages.compress import CompressedFileMixin
from storages.utils import check_location
from storages.utils import clean_name
from storages.utils import safe_join
from storages.utils import setting
from storages.utils import to_bytes

try:
    from google.cloud.exceptions import NotFound
    from google.cloud.storage import Blob
    from google.cloud.storage import Client
    from google.cloud.storage.blob import _quote
    from google.cloud.storage.retry import DEFAULT_RETRY
except ImportError:
    raise ImproperlyConfigured(
        "Could not load Google Cloud Storage bindings.\n"
        "See https://github.com/GoogleCloudPlatform/gcloud-python"
    )


CONTENT_ENCODING = "content_encoding"
CONTENT_TYPE = "content_type"


class GoogleCloudFile(CompressedFileMixin, File):
    def __init__(self, name, mode, storage):
        self.name = name
        self.mime_type, self.mime_encoding = mimetypes.guess_type(name)
        self._mode = mode
        self._storage = storage
        self.blob = storage.bucket.get_blob(name, chunk_size=storage.blob_chunk_size)
        if not self.blob and "w" in mode:
            self.blob = Blob(
                self.name, storage.bucket, chunk_size=storage.blob_chunk_size
            )
        self._file = None
        self._is_dirty = False

    @property
    def size(self):
        return self.blob.size

    def _get_file(self):
        if self._file is None:
            self._file = SpooledTemporaryFile(
                max_size=self._storage.max_memory_size,
                suffix=".GSStorageFile",
                dir=setting("FILE_UPLOAD_TEMP_DIR"),
            )
            if "r" in self._mode:
                self._is_dirty = False
                self.blob.download_to_file(self._file)
                self._file.seek(0)
            if self._storage.gzip and self.blob.content_encoding == "gzip":
                self._file = self._decompress_file(mode=self._mode, file=self._file)
        return self._file

    def _set_file(self, value):
        self._file = value

    file = property(_get_file, _set_file)

    def read(self, num_bytes=None):
        if "r" not in self._mode:
            raise AttributeError("File was not opened in read mode.")

        if num_bytes is None:
            num_bytes = -1

        return super().read(num_bytes)

    def write(self, content):
        if "w" not in self._mode:
            raise AttributeError("File was not opened in write mode.")
        self._is_dirty = True
        return super().write(to_bytes(content))

    def close(self):
        if self._file is not None:
            if self._is_dirty:
                blob_params = self._storage.get_object_parameters(self.name)
                self.blob.upload_from_file(
                    self.file,
                    rewind=True,
                    content_type=self.mime_type,
                    retry=DEFAULT_RETRY,
                    predefined_acl=blob_params.get("acl", self._storage.default_acl),
                )
            self._file.close()
            self._file = None


@deconstructible
class GoogleCloudStorage(BaseStorage):
    def __init__(self, **settings):
        super().__init__(**settings)

        check_location(self)

        self._bucket = None
        self._client = None

    def get_default_settings(self):
        return {
            "project_id": setting("GS_PROJECT_ID"),
            "credentials": setting("GS_CREDENTIALS"),
            "bucket_name": setting("GS_BUCKET_NAME"),
            "custom_endpoint": setting("GS_CUSTOM_ENDPOINT", None),
            "location": setting("GS_LOCATION", ""),
            "default_acl": setting("GS_DEFAULT_ACL"),
            "querystring_auth": setting("GS_QUERYSTRING_AUTH", True),
            "expiration": setting("GS_EXPIRATION", timedelta(seconds=86400)),
            "gzip": setting("GS_IS_GZIPPED", False),
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
            "file_overwrite": setting("GS_FILE_OVERWRITE", True),
            "object_parameters": setting("GS_OBJECT_PARAMETERS", {}),
            # The max amount of memory a returned file can take up before being
            # rolled over into a temporary file on disk. Default is 0: Do not
            # roll over.
            "max_memory_size": setting("GS_MAX_MEMORY_SIZE", 0),
            "blob_chunk_size": setting("GS_BLOB_CHUNK_SIZE"),
        }

    @property
    def client(self):
        if self._client is None:
            self._client = Client(project=self.project_id, credentials=self.credentials)
        return self._client

    @property
    def bucket(self):
        if self._bucket is None:
            self._bucket = self.client.bucket(self.bucket_name)
        return self._bucket

    def _normalize_name(self, name):
        """
        Normalizes the name so that paths like /path/to/ignored/../something.txt
        and ./file.txt work.  Note that clean_name adds ./ to some paths so
        they need to be fixed here. We check to make sure that the path pointed
        to is not outside the directory specified by the LOCATION setting.
        """
        try:
            return safe_join(self.location, name)
        except ValueError:
            raise SuspiciousOperation("Attempted access to '%s' denied." % name)

    def _open(self, name, mode="rb"):
        name = self._normalize_name(clean_name(name))
        file_object = GoogleCloudFile(name, mode, self)
        if not file_object.blob:
            raise FileNotFoundError("File does not exist: %s" % name)
        return file_object

    def _compress_content(self, content):
        content.seek(0)
        zbuf = io.BytesIO()
        with gzip.GzipFile(mode="wb", fileobj=zbuf, mtime=0.0) as zfile:
            zfile.write(to_bytes(content.read()))
        zbuf.seek(0)
        return zbuf

    def _save(self, name, content):
        cleaned_name = clean_name(name)
        name = self._normalize_name(cleaned_name)

        content.name = cleaned_name
        file_object = GoogleCloudFile(name, "rw", self)

        blob_params = self.get_object_parameters(name)
        if file_object.mime_encoding and CONTENT_ENCODING not in blob_params:
            blob_params[CONTENT_ENCODING] = file_object.mime_encoding

        upload_params = {}
        upload_params["predefined_acl"] = blob_params.pop("acl", self.default_acl)
        upload_params[CONTENT_TYPE] = blob_params.pop(
            CONTENT_TYPE, file_object.mime_type
        )

        if (
            self.gzip
            and upload_params[CONTENT_TYPE] in self.gzip_content_types
            and CONTENT_ENCODING not in blob_params
        ):
            content = self._compress_content(content)
            blob_params[CONTENT_ENCODING] = "gzip"

        for prop, val in blob_params.items():
            setattr(file_object.blob, prop, val)

        file_object.blob.upload_from_file(
            content,
            rewind=True,
            retry=DEFAULT_RETRY,
            size=getattr(content, "size", None),
            **upload_params,
        )
        return cleaned_name

    def get_object_parameters(self, name):
        """Override this to return a dictionary of overwritable blob-property to value.

        Returns GS_OBJECT_PARAMETERS by default. See the docs for all possible options.
        """
        object_parameters = self.object_parameters.copy()
        return object_parameters

    def delete(self, name):
        name = self._normalize_name(clean_name(name))
        try:
            self.bucket.delete_blob(name, retry=DEFAULT_RETRY)
        except NotFound:
            pass

    def exists(self, name):
        if not name:  # root element aka the bucket
            try:
                self.client.get_bucket(self.bucket)
                return True
            except NotFound:
                return False

        if self.file_overwrite:
            return False

        name = self._normalize_name(clean_name(name))
        return bool(self.bucket.get_blob(name))

    def listdir(self, name):
        name = self._normalize_name(clean_name(name))
        # For bucket.list_blobs and logic below name needs to end in /
        # but for the root path "" we leave it as an empty string
        if name and not name.endswith("/"):
            name += "/"

        iterator = self.bucket.list_blobs(prefix=name, delimiter="/")
        blobs = list(iterator)
        prefixes = iterator.prefixes

        files = []
        dirs = []

        for blob in blobs:
            parts = blob.name.split("/")
            files.append(parts[-1])
        for folder_path in prefixes:
            parts = folder_path.split("/")
            dirs.append(parts[-2])

        return list(dirs), files

    def _get_blob(self, name):
        # Wrap google.cloud.storage's blob to raise if the file doesn't exist
        blob = self.bucket.get_blob(name)

        if blob is None:
            raise NotFound("File does not exist: {}".format(name))

        return blob

    def size(self, name):
        name = self._normalize_name(clean_name(name))
        blob = self._get_blob(name)
        return blob.size

    def get_modified_time(self, name):
        name = self._normalize_name(clean_name(name))
        blob = self._get_blob(name)
        updated = blob.updated
        return updated if setting("USE_TZ") else timezone.make_naive(updated)

    def get_created_time(self, name):
        """
        Return the creation time (as a datetime) of the file specified by name.
        The datetime will be timezone-aware if USE_TZ=True.
        """
        name = self._normalize_name(clean_name(name))
        blob = self._get_blob(name)
        created = blob.time_created
        return created if setting("USE_TZ") else timezone.make_naive(created)

    def url(self, name, parameters=None):
        """
        Return public URL or a signed URL for the Blob.

        To keep things snappy, the existence of blobs for public URLs is not checked.
        """
        name = self._normalize_name(clean_name(name))
        blob = self.bucket.blob(name)
        blob_params = self.get_object_parameters(name)
        no_signed_url = (
            blob_params.get("acl", self.default_acl) == "publicRead"
            or not self.querystring_auth
        )

        if not self.custom_endpoint and no_signed_url:
            return blob.public_url
        elif no_signed_url:
            return "{storage_base_url}/{quoted_name}".format(
                storage_base_url=self.custom_endpoint,
                quoted_name=_quote(name, safe=b"/~"),
            )
        else:
            default_params = {
                "bucket_bound_hostname": self.custom_endpoint,
                "expiration": self.expiration,
                "version": "v4",
            }
            params = parameters or {}

            for key, value in default_params.items():
                if value and key not in params:
                    params[key] = value

            return blob.generate_signed_url(**params)
