# Dropbox storage class for Django pluggable storage system.
# Author: Anthony Monthe <anthony.monthe@gmail.com>
# License: BSD
#
# Usage:
#
# Add below to settings.py:
# DROPBOX_OAUTH2_TOKEN = 'YourOauthToken'
# DROPBOX_ROOT_PATH = '/dir/'

from __future__ import absolute_import

from io import BytesIO
from shutil import copyfileobj
from tempfile import SpooledTemporaryFile

from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import File
from django.core.files.storage import Storage
from django.utils._os import safe_join
from django.utils.deconstruct import deconstructible
from dropbox import Dropbox
from dropbox.exceptions import ApiError
from dropbox.files import CommitInfo, FolderMetadata, UploadSessionCursor

from storages.utils import setting

_DEFAULT_TIMEOUT = 100


class DropBoxStorageException(Exception):
    pass


class DropBoxFile(File):
    def __init__(self, name, storage):
        self.name = name
        self._storage = storage
        self._file = None

    def _get_file(self):
        if self._file is None:
            self._file = SpooledTemporaryFile()
            # As dropbox==9.3.0, the client returns a tuple
            # (dropbox.files.FileMetadata, requests.models.Response)
            file_metadata, response = \
                self._storage.client.files_download(self.name)
            if response.status_code == 200:
                with BytesIO(response.content) as file_content:
                    copyfileobj(file_content, self._file)
            else:
                # JIC the exception isn't catched by the dropbox client
                raise DropBoxStorageException(
                    "Dropbox server returned a {} response when accessing {}"
                    .format(response.status_code, self.name)
                )
            self._file.seek(0)
        return self._file

    def _set_file(self, value):
        self._file = value

    file = property(_get_file, _set_file)


@deconstructible
class DropBoxStorage(Storage):
    """DropBox Storage class for Django pluggable storage system."""
    location = setting('DROPBOX_ROOT_PATH', '/')
    oauth2_access_token = setting('DROPBOX_OAUTH2_TOKEN')
    timeout = setting('DROPBOX_TIMEOUT', _DEFAULT_TIMEOUT)

    CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self, oauth2_access_token=oauth2_access_token, root_path=location, timeout=timeout):
        if oauth2_access_token is None:
            raise ImproperlyConfigured("You must configure an auth token at"
                                       "'settings.DROPBOX_OAUTH2_TOKEN'.")

        self.root_path = root_path
        self.client = Dropbox(oauth2_access_token, timeout=timeout)

    def _full_path(self, name):
        if name == '/':
            name = ''
        return safe_join(self.root_path, name).replace('\\', '/')

    def delete(self, name):
        self.client.files_delete(self._full_path(name))

    def exists(self, name):
        try:
            return bool(self.client.files_get_metadata(self._full_path(name)))
        except ApiError:
            return False

    def listdir(self, path):
        directories, files = [], []
        full_path = self._full_path(path)

        if full_path == '/':
            full_path = ''

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

    def modified_time(self, name):
        metadata = self.client.files_get_metadata(self._full_path(name))
        return metadata.server_modified

    def accessed_time(self, name):
        metadata = self.client.files_get_metadata(self._full_path(name))
        return metadata.client_modified

    def url(self, name):
        media = self.client.files_get_temporary_link(self._full_path(name))
        return media.link

    def _open(self, name, mode='rb'):
        remote_file = DropBoxFile(self._full_path(name), self)
        return remote_file

    def _save(self, name, content):
        content.open()
        if content.size <= self.CHUNK_SIZE:
            self.client.files_upload(content.read(), self._full_path(name))
        else:
            self._chunked_upload(content, self._full_path(name))
        content.close()
        return name

    def _chunked_upload(self, content, dest_path):
        upload_session = self.client.files_upload_session_start(
            content.read(self.CHUNK_SIZE)
        )
        cursor = UploadSessionCursor(
            session_id=upload_session.session_id,
            offset=content.tell()
        )
        commit = CommitInfo(path=dest_path)

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
