# Dropbox storage class for Django pluggable storage system.
# Author: Anthony Monthe <anthony.monthe@gmail.com>
# License: BSD

import warnings
from io import BytesIO
from shutil import copyfileobj
from tempfile import SpooledTemporaryFile

from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import File
from django.utils._os import safe_join
from django.utils.deconstruct import deconstructible
from dropbox import Dropbox
from dropbox.exceptions import ApiError
from dropbox.files import CommitInfo
from dropbox.files import FolderMetadata
from dropbox.files import UploadSessionCursor
from dropbox.files import WriteMode

from storages.base import BaseStorage
from storages.utils import setting

_DEFAULT_TIMEOUT = 100
_DEFAULT_MODE = "add"


class DropboxStorageException(Exception):
    pass


DropBoxStorageException = DropboxStorageException


class DropboxFile(File):
    def __init__(self, name, storage):
        self.name = name
        self._storage = storage
        self._file = None

    def _get_file(self):
        if self._file is None:
            self._file = SpooledTemporaryFile()
            # As dropbox==9.3.0, the client returns a tuple
            # (dropbox.files.FileMetadata, requests.models.Response)
            file_metadata, response = self._storage.client.files_download(self.name)
            if response.status_code == 200:
                with BytesIO(response.content) as file_content:
                    copyfileobj(file_content, self._file)
            else:
                # JIC the exception isn't catched by the dropbox client
                raise DropboxStorageException(
                    "Dropbox server returned a {} response when accessing {}".format(
                        response.status_code, self.name
                    )
                )
            self._file.seek(0)
        return self._file

    def _set_file(self, value):
        self._file = value

    file = property(_get_file, _set_file)


DropBoxFile = DropboxFile


@deconstructible
class DropboxStorage(BaseStorage):
    """Dropbox Storage class for Django pluggable storage system."""

    CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self, oauth2_access_token=None, **settings):
        super().__init__(oauth2_access_token=oauth2_access_token, **settings)

        if self.oauth2_access_token is None and not all(
            [self.app_key, self.app_secret, self.oauth2_refresh_token]
        ):
            raise ImproperlyConfigured(
                "You must configure an auth token at"
                "'settings.DROPBOX_OAUTH2_TOKEN' or "
                "'setting.DROPBOX_APP_KEY', "
                "'setting.DROPBOX_APP_SECRET' "
                "and 'setting.DROPBOX_OAUTH2_REFRESH_TOKEN'."
            )
        self.client = Dropbox(
            self.oauth2_access_token,
            app_key=self.app_key,
            app_secret=self.app_secret,
            oauth2_refresh_token=self.oauth2_refresh_token,
            timeout=self.timeout,
        )

        # Backwards compat
        if hasattr(self, "location"):
            warnings.warn(
                "Setting `root_path` with name `location` is deprecated and will be "
                "removed in a future version of django-storages. Please update the "
                "name from `location` to `root_path`",
                DeprecationWarning,
            )
            self.root_path = self.location

    def get_default_settings(self):
        return {
            "root_path": setting("DROPBOX_ROOT_PATH", "/"),
            "oauth2_access_token": setting("DROPBOX_OAUTH2_TOKEN"),
            "app_key": setting("DROPBOX_APP_KEY"),
            "app_secret": setting("DROPBOX_APP_SECRET"),
            "oauth2_refresh_token": setting("DROPBOX_OAUTH2_REFRESH_TOKEN"),
            "timeout": setting("DROPBOX_TIMEOUT", _DEFAULT_TIMEOUT),
            "write_mode": setting("DROPBOX_WRITE_MODE", _DEFAULT_MODE),
        }

    def _full_path(self, name):
        if name == "/":
            name = ""
        return safe_join(self.root_path, name).replace("\\", "/")

    def delete(self, name):
        self.client.files_delete(self._full_path(name))

    def exists(self, name):
        if self.write_mode == "overwrite":
            return False

        try:
            return bool(self.client.files_get_metadata(self._full_path(name)))
        except ApiError:
            return False

    def listdir(self, path):
        directories, files = [], []
        full_path = self._full_path(path)

        if full_path == "/":
            full_path = ""

        metadata = self.client.files_list_folder(full_path)
        for entry in metadata.entries:
            if isinstance(entry, FolderMetadata):
                directories.append(entry.name)
            else:
                files.append(entry.name)
        return directories, files

    def size(self, name):
        metadata = self.client.files_get_metadata(self._full_path(name))
        return metadata.size

    def url(self, name):
        try:
            media = self.client.files_get_temporary_link(self._full_path(name))
            return media.link
        except ApiError:
            return None

    def _open(self, name, mode="rb"):
        remote_file = DropboxFile(self._full_path(name), self)
        return remote_file

    def _save(self, name, content):
        content.open()
        if content.size <= self.CHUNK_SIZE:
            self.client.files_upload(
                content.read(), self._full_path(name), mode=WriteMode(self.write_mode)
            )
        else:
            self._chunked_upload(content, self._full_path(name))
        content.close()
        return name

    def _chunked_upload(self, content, dest_path):
        upload_session = self.client.files_upload_session_start(
            content.read(self.CHUNK_SIZE)
        )
        cursor = UploadSessionCursor(
            session_id=upload_session.session_id, offset=content.tell()
        )
        commit = CommitInfo(path=dest_path, mode=WriteMode(self.write_mode))

        while content.tell() < content.size:
            if (content.size - content.tell()) <= self.CHUNK_SIZE:
                self.client.files_upload_session_finish(
                    content.read(self.CHUNK_SIZE), cursor, commit
                )
            else:
                self.client.files_upload_session_append_v2(
                    content.read(self.CHUNK_SIZE), cursor
                )
                cursor.offset = content.tell()


DropBoxStorage = DropboxStorage
