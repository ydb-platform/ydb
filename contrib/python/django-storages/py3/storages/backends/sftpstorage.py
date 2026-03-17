# SFTP storage backend for Django.
# Author: Brent Tubbs <brent.tubbs@gmail.com>
# License: MIT
#
# Modeled on the FTP storage by Rafal Jonca <jonca.rafal@gmail.com>

import datetime
import getpass
import io
import os
import posixpath
import stat
from urllib.parse import urljoin

import paramiko
from django.core.files.base import File
from django.utils.deconstruct import deconstructible
from paramiko.util import ClosingContextManager

from storages.base import BaseStorage
from storages.utils import is_seekable
from storages.utils import setting


@deconstructible
class SFTPStorage(ClosingContextManager, BaseStorage):
    def __init__(self, **settings):
        super().__init__(**settings)
        self._ssh = None
        self._sftp = None

    def get_default_settings(self):
        return {
            "host": setting("SFTP_STORAGE_HOST"),
            "params": setting("SFTP_STORAGE_PARAMS", {}),
            "interactive": setting("SFTP_STORAGE_INTERACTIVE", False),
            "file_mode": setting("SFTP_STORAGE_FILE_MODE"),
            "dir_mode": setting("SFTP_STORAGE_DIR_MODE"),
            "uid": setting("SFTP_STORAGE_UID"),
            "gid": setting("SFTP_STORAGE_GID"),
            "known_host_file": setting("SFTP_KNOWN_HOST_FILE"),
            "root_path": setting("SFTP_STORAGE_ROOT", ""),
            "base_url": setting("SFTP_BASE_URL") or setting("MEDIA_URL"),
        }

    def _connect(self):
        self._ssh = paramiko.SSHClient()

        known_host_file = self.known_host_file or os.path.expanduser(
            os.path.join("~", ".ssh", "known_hosts")
        )

        if os.path.exists(known_host_file):
            self._ssh.load_host_keys(known_host_file)

        # and automatically add new host keys for hosts we haven't seen before.
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self._ssh.connect(self.host, **self.params)
        except paramiko.AuthenticationException as e:
            if self.interactive and "password" not in self.params:
                # If authentication has failed, and we haven't already tried
                # username/password, and configuration allows it, then try
                # again with username/password.
                if "username" not in self.params:
                    self.params["username"] = getpass.getuser()
                self.params["password"] = getpass.getpass()
                self._connect()
            else:
                raise paramiko.AuthenticationException(e)

        if self._ssh.get_transport():
            self._sftp = self._ssh.open_sftp()

    def close(self):
        if self._ssh is None:
            return
        self._ssh.close()

    @property
    def sftp(self):
        """Lazy SFTP connection"""
        if not self._sftp or not self._ssh.get_transport().is_active():
            self._connect()
        return self._sftp

    def _remote_path(self, name):
        return posixpath.join(self.root_path, name)

    def _open(self, name, mode="rb"):
        return SFTPStorageFile(name, self, mode)

    def _read(self, name):
        remote_path = self._remote_path(name)
        return self.sftp.open(remote_path, "rb")

    def _chown(self, path, uid=None, gid=None):
        """Set uid and/or gid for file at path."""
        # Paramiko's chown requires both uid and gid, so look them up first if
        # we're only supposed to set one.
        if uid is None or gid is None:
            attr = self.sftp.stat(path)
            uid = uid or attr.st_uid
            gid = gid or attr.st_gid
        self.sftp.chown(path, uid, gid)

    def _mkdir(self, path):
        """Create directory, recursing up to create parent dirs if
        necessary."""
        parent = posixpath.dirname(path)
        if not self._path_exists(parent):
            self._mkdir(parent)
        self.sftp.mkdir(path)

        if self.dir_mode is not None:
            self.sftp.chmod(path, self.dir_mode)

        if self.uid or self.gid:
            self._chown(path, uid=self.uid, gid=self.gid)

    def _save(self, name, content):
        """Save file via SFTP."""
        if is_seekable(content):
            content.seek(0, os.SEEK_SET)
        path = self._remote_path(name)
        dirname = posixpath.dirname(path)
        if not self._path_exists(dirname):
            self._mkdir(dirname)

        self.sftp.putfo(content, path)

        # set file permissions if configured
        if self.file_mode is not None:
            self.sftp.chmod(path, self.file_mode)
        if self.uid or self.gid:
            self._chown(path, uid=self.uid, gid=self.gid)
        return name

    def delete(self, name):
        try:
            self.sftp.remove(self._remote_path(name))
        except OSError:
            pass

    def _path_exists(self, path):
        """Determines whether a file existis in the sftp storage given its
        absolute path."""
        try:
            self.sftp.stat(path)
            return True
        except FileNotFoundError:
            return False

    def exists(self, name):
        """Determines whether a file exists within the root folder of the SFTP storage
        (as set by `SFTP_STORAGE_ROOT`). This method differs from `._path_exists()`
        in that the provided `name` is assumed to be the relative path of the file
        within the root folder.
        """
        return self._path_exists(self._remote_path(name))

    def _isdir_attr(self, item):
        # Return whether an item in sftp.listdir_attr results is a directory
        if item.st_mode is not None:
            return stat.S_IFMT(item.st_mode) == stat.S_IFDIR
        else:
            return False

    def listdir(self, path):
        remote_path = self._remote_path(path)
        dirs, files = [], []
        for item in self.sftp.listdir_attr(remote_path):
            if self._isdir_attr(item):
                dirs.append(item.filename)
            else:
                files.append(item.filename)
        return dirs, files

    def size(self, name):
        remote_path = self._remote_path(name)
        return self.sftp.stat(remote_path).st_size

    # From Django
    def _datetime_from_timestamp(self, ts):
        tz = datetime.timezone.utc if setting("USE_TZ") else None
        return datetime.datetime.fromtimestamp(ts, tz=tz)

    def get_accessed_time(self, name):
        remote_path = self._remote_path(name)
        utime = self.sftp.stat(remote_path).st_atime
        return self._datetime_from_timestamp(utime)

    def get_modified_time(self, name):
        remote_path = self._remote_path(name)
        utime = self.sftp.stat(remote_path).st_mtime
        return self._datetime_from_timestamp(utime)

    def url(self, name):
        if self.base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        return urljoin(self.base_url, name).replace("\\", "/")


class SFTPStorageFile(File):
    def __init__(self, name, storage, mode):
        self.name = name
        self.mode = mode
        self.file = io.BytesIO()
        self._storage = storage
        self._is_read = False
        self._is_dirty = False

    @property
    def size(self):
        if not hasattr(self, "_size"):
            self._size = self._storage.size(self.name)
        return self._size

    def read(self, num_bytes=None):
        if not self._is_read:
            self.file = self._storage._read(self.name)
            self._is_read = True

        return self.file.read(num_bytes)

    def write(self, content):
        if "w" not in self.mode:
            raise AttributeError("File was opened for read-only access.")
        self.file = io.BytesIO(content)
        self._is_dirty = True
        self._is_read = True

    def open(self, mode=None):
        if not self.closed:
            self.seek(0)
        elif self.name and self._storage.exists(self.name):
            self.file = self._storage._open(self.name, mode or self.mode)
        else:
            raise ValueError("The file cannot be reopened.")

    def close(self):
        if self._is_dirty:
            self._storage._save(self.name, self)
        self.file.close()
