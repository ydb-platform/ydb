import mimetypes
from datetime import datetime
from datetime import timedelta
from tempfile import SpooledTemporaryFile
from urllib.parse import urlparse
from urllib.parse import urlunparse

from azure.core.exceptions import ResourceNotFoundError
from azure.core.utils import parse_connection_string
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobSasPermissions
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings
from azure.storage.blob import generate_blob_sas
from django.core.exceptions import SuspiciousOperation
from django.core.files.base import File
from django.utils import timezone
from django.utils.deconstruct import deconstructible

from storages.base import BaseStorage
from storages.utils import clean_name
from storages.utils import safe_join
from storages.utils import setting
from storages.utils import to_bytes


@deconstructible
class AzureStorageFile(File):
    def __init__(self, name, mode, storage):
        self.name = name
        self._mode = mode
        self._storage = storage
        self._is_dirty = False
        self._file = None
        self._path = storage._get_valid_path(name)

    def _get_file(self):
        if self._file is not None:
            return self._file

        file = SpooledTemporaryFile(
            max_size=self._storage.max_memory_size,
            suffix=".AzureStorageFile",
            dir=setting("FILE_UPLOAD_TEMP_DIR", None),
        )

        if "r" in self._mode or "a" in self._mode:
            download_stream = self._storage.client.download_blob(
                self._path, timeout=self._storage.timeout
            )
            download_stream.readinto(file)
        if "r" in self._mode:
            file.seek(0)

        self._file = file
        return self._file

    def _set_file(self, value):
        self._file = value

    file = property(_get_file, _set_file)

    def read(self, *args, **kwargs):
        if "r" not in self._mode and "a" not in self._mode:
            raise AttributeError("File was not opened in read mode.")
        return super().read(*args, **kwargs)

    def write(self, content):
        if "w" not in self._mode and "+" not in self._mode and "a" not in self._mode:
            raise AttributeError("File was not opened in write mode.")
        self._is_dirty = True
        return super().write(to_bytes(content))

    def close(self):
        if self._file is None:
            return
        if self._is_dirty:
            self._file.seek(0)
            self._storage._save(self.name, self._file)
            self._is_dirty = False
        self._file.close()
        self._file = None


def _content_type(content):
    try:
        return content.file.content_type
    except AttributeError:
        pass
    try:
        return content.content_type
    except AttributeError:
        pass
    return None


def _get_valid_path(s):
    # A blob name:
    #   * must not end with dot or slash
    #   * can contain any character
    #   * must escape URL reserved characters
    #     (not needed here since the azure client will do that)
    s = s.strip("./")
    if len(s) > _AZURE_NAME_MAX_LEN:
        raise ValueError("File name max len is %d" % _AZURE_NAME_MAX_LEN)
    if not len(s):
        raise ValueError("File name must contain one or more printable characters")
    if s.count("/") > 256:
        raise ValueError("File name must not contain more than 256 slashes")
    return s


# Max len according to azure's docs
_AZURE_NAME_MAX_LEN = 1024


@deconstructible
class AzureStorage(BaseStorage):
    def __init__(self, **settings):
        super().__init__(**settings)
        self._service_client = None
        self._client = None
        self._user_delegation_key = None
        self._user_delegation_key_expiry = datetime.utcnow()
        if self.connection_string and (not self.account_name or not self.account_key):
            parsed = parse_connection_string(
                self.connection_string, case_sensitive_keys=True
            )
            if not self.account_name and "AccountName" in parsed:
                self.account_name = parsed["AccountName"]
            if not self.account_key and "AccountKey" in parsed:
                self.account_key = parsed["AccountKey"]

    def get_default_settings(self):
        return {
            "account_name": setting("AZURE_ACCOUNT_NAME"),
            "account_key": setting("AZURE_ACCOUNT_KEY"),
            "object_parameters": setting("AZURE_OBJECT_PARAMETERS", {}),
            "azure_container": setting("AZURE_CONTAINER"),
            "azure_ssl": setting("AZURE_SSL", True),
            "upload_max_conn": setting("AZURE_UPLOAD_MAX_CONN", 2),
            "timeout": setting("AZURE_CONNECTION_TIMEOUT_SECS", 20),
            "max_memory_size": setting("AZURE_BLOB_MAX_MEMORY_SIZE", 2 * 1024 * 1024),
            "expiration_secs": setting("AZURE_URL_EXPIRATION_SECS"),
            "overwrite_files": setting("AZURE_OVERWRITE_FILES", False),
            "location": setting("AZURE_LOCATION", ""),
            "default_content_type": "application/octet-stream",
            "cache_control": setting("AZURE_CACHE_CONTROL"),
            "sas_token": setting("AZURE_SAS_TOKEN"),
            "endpoint_suffix": setting("AZURE_ENDPOINT_SUFFIX", "core.windows.net"),
            "custom_domain": setting("AZURE_CUSTOM_DOMAIN"),
            "connection_string": setting("AZURE_CONNECTION_STRING"),
            "token_credential": setting("AZURE_TOKEN_CREDENTIAL"),
            "api_version": setting("AZURE_API_VERSION", None),
        }

    def _get_service_client(self):
        if self.connection_string is not None:
            return BlobServiceClient.from_connection_string(self.connection_string)

        account_domain = "{}.blob.{}".format(self.account_name, self.endpoint_suffix)
        account_url = "{}://{}".format(self.azure_protocol, account_domain)

        credential = None
        if self.account_key:
            credential = {
                "account_name": self.account_name,
                "account_key": self.account_key,
            }
        elif self.sas_token:
            credential = self.sas_token
        elif self.token_credential:
            credential = self.token_credential
        options = {}
        if self.api_version:
            options["api_version"] = self.api_version
        return BlobServiceClient(account_url, credential=credential, **options)

    @property
    def service_client(self):
        if self._service_client is None:
            self._service_client = self._get_service_client()
        return self._service_client

    @property
    def client(self):
        if self._client is None:
            self._client = self.service_client.get_container_client(
                self.azure_container
            )
        return self._client

    def get_user_delegation_key(self, expiry):
        # We'll only be able to get a user delegation key if we've authenticated with a
        # token credential.
        if self.token_credential is None:
            return None

        # Get a new key if we don't already have one, or if the one we have expires too
        # soon.
        if (
            self._user_delegation_key is None
            or expiry > self._user_delegation_key_expiry
        ):
            now = datetime.utcnow()
            key_expiry_time = now + timedelta(days=7)
            self._user_delegation_key = self.service_client.get_user_delegation_key(
                key_start_time=now, key_expiry_time=key_expiry_time
            )
            self._user_delegation_key_expiry = key_expiry_time

        return self._user_delegation_key

    @property
    def azure_protocol(self):
        if self.azure_ssl:
            return "https"
        else:
            return "http"

    def _normalize_name(self, name):
        try:
            return safe_join(self.location, name)
        except ValueError:
            raise SuspiciousOperation("Attempted access to '%s' denied." % name)

    def _get_valid_path(self, name):
        # Must be idempotent
        return _get_valid_path(self._normalize_name(clean_name(name)))

    def _open(self, name, mode="rb"):
        return AzureStorageFile(name, mode, self)

    def exists(self, name):
        if not name:
            return True

        if self.overwrite_files:
            return False

        blob_client = self.client.get_blob_client(self._get_valid_path(name))
        return blob_client.exists()

    def delete(self, name):
        try:
            self.client.delete_blob(self._get_valid_path(name), timeout=self.timeout)
        except ResourceNotFoundError:
            pass

    def size(self, name):
        blob_client = self.client.get_blob_client(self._get_valid_path(name))
        properties = blob_client.get_blob_properties(timeout=self.timeout)
        return properties.size

    def _save(self, name, content):
        cleaned_name = clean_name(name)
        name = self._get_valid_path(name)
        params = self._get_content_settings_parameters(name, content)

        # Unwrap django file (wrapped by parent's save call)
        if isinstance(content, File):
            content = content.file

        content.seek(0)
        self.client.upload_blob(
            name,
            content,
            content_settings=ContentSettings(**params),
            max_concurrency=self.upload_max_conn,
            timeout=self.timeout,
            overwrite=self.overwrite_files,
        )
        return cleaned_name

    def _expire_at(self, expire):
        # azure expects time in UTC
        return datetime.utcnow() + timedelta(seconds=expire)

    def url(self, name, expire=None, parameters=None, mode="r"):
        name = self._get_valid_path(name)
        params = parameters or {}
        permission = BlobSasPermissions.from_string(mode)

        if expire is None:
            expire = self.expiration_secs

        credential = None
        if expire:
            expiry = self._expire_at(expire)
            user_delegation_key = self.get_user_delegation_key(expiry)
            sas_token = generate_blob_sas(
                self.account_name,
                self.azure_container,
                name,
                account_key=self.account_key,
                user_delegation_key=user_delegation_key,
                permission=permission,
                expiry=expiry,
                **params,
            )
            credential = sas_token

        container_blob_url = self.client.get_blob_client(name).url

        if self.custom_domain:
            # Replace the account name with the custom domain
            parsed_url = urlparse(container_blob_url)
            container_blob_url = urlunparse(
                parsed_url._replace(netloc=self.custom_domain)
            )

        return BlobClient.from_blob_url(container_blob_url, credential=credential).url

    def _get_content_settings_parameters(self, name, content=None):
        params = {}

        guessed_type, content_encoding = mimetypes.guess_type(name)
        content_type = (
            _content_type(content) or guessed_type or self.default_content_type
        )

        params["cache_control"] = self.cache_control
        params["content_type"] = content_type
        params["content_encoding"] = content_encoding

        params.update(self.get_object_parameters(name))
        return params

    def get_object_parameters(self, name):
        """
        Returns a dictionary that is passed to content settings. Override this
        method to adjust this on a per-object basis to set e.g ContentDisposition.

        By default, returns the value of AZURE_OBJECT_PARAMETERS.
        """
        return self.object_parameters.copy()

    def get_modified_time(self, name):
        """
        Returns an (aware) datetime object containing the last modified time if
        USE_TZ is True, otherwise returns a naive datetime in the local timezone.
        """
        blob_client = self.client.get_blob_client(self._get_valid_path(name))
        properties = blob_client.get_blob_properties(timeout=self.timeout)
        if not setting("USE_TZ", False):
            return timezone.make_naive(properties.last_modified)

        tz = timezone.get_current_timezone()
        if timezone.is_naive(properties.last_modified):
            return timezone.make_aware(properties.last_modified, tz)

        # `last_modified` is in UTC time_zone, we
        # must convert it to settings time_zone
        return properties.last_modified.astimezone(tz)

    def list_all(self, path=""):
        """Return all files for a given path"""
        if path:
            path = self._get_valid_path(path)
        if path and not path.endswith("/"):
            path += "/"
        # XXX make generator, add start, end
        return [
            blob.name
            for blob in self.client.list_blobs(
                name_starts_with=path, timeout=self.timeout
            )
        ]

    def listdir(self, path=""):
        """
        Return all files for a given path.
        Given that Azure can't return paths it only returns files.
        Works great for our little adventure.
        """

        return [], self.list_all(path)
