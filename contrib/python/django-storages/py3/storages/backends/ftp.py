# FTP storage class for Django pluggable storage system.
# Author: Rafal Jonca <jonca.rafal@gmail.com>
# License: MIT
# Comes from http://www.djangosnippets.org/snippets/1269/
#
# Usage:
#
# Add below to settings.py:
# FTP_STORAGE_LOCATION = '[a]ftp[s]://<user>:<pass>@<host>:<port>/[path]'
#
# In models.py you can write:
# from FTPStorage import FTPStorage
# fs = FTPStorage()
# For a TLS configuration, you must use 'ftps' protocol
# class FTPTest(models.Model):
#     file = models.FileField(upload_to='a/b/c/', storage=fs)

import ftplib
import io
import os
import re
import urllib.parse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import File
from django.utils.deconstruct import deconstructible

from storages.base import BaseStorage
from storages.utils import setting


class FTPStorageException(Exception):
    pass


@deconstructible
class FTPStorage(BaseStorage):
    """FTP Storage class for Django pluggable storage system."""

    def __init__(self, **settings):
        super().__init__(**settings)
        if self.location is None:
            raise ImproperlyConfigured(
                "You must set a location at instantiation "
                "or at settings.FTP_STORAGE_LOCATION."
            )
        self._config = self._decode_location(self.location)
        self._connection = None

    def get_default_settings(self):
        return {
            "location": setting("FTP_STORAGE_LOCATION"),
            "encoding": setting("FTP_STORAGE_ENCODING", "latin-1"),
            "base_url": setting("BASE_URL", settings.MEDIA_URL),
            "allow_overwrite": setting("FTP_ALLOW_OVERWRITE", False),
        }

    def _decode_location(self, location):
        """Return splitted configuration data from location."""
        splitted_url = re.search(
            r"^(?P<scheme>.+)://(?P<user>.+):(?P<passwd>.+)@"
            r"(?P<host>.+):(?P<port>\d+)/(?P<path>.*)$",
            location,
        )

        if splitted_url is None:
            raise ImproperlyConfigured("Improperly formatted location URL")
        if splitted_url["scheme"] not in ("ftp", "aftp", "ftps"):
            raise ImproperlyConfigured("Only ftp, aftp, ftps schemes supported")
        if splitted_url["host"] == "":
            raise ImproperlyConfigured("You must at least provide host!")

        config = {}
        config["active"] = splitted_url["scheme"] == "aftp"
        config["secure"] = splitted_url["scheme"] == "ftps"

        config["path"] = splitted_url["path"] or "/"
        config["host"] = splitted_url["host"]
        config["user"] = splitted_url["user"]
        config["passwd"] = splitted_url["passwd"]
        config["port"] = int(splitted_url["port"])

        return config

    def _start_connection(self):
        # Check if connection is still alive and if not, drop it.
        if self._connection is not None:
            try:
                self._connection.pwd()
            except ftplib.all_errors:
                self._connection = None

        # Real reconnect
        if self._connection is None:
            ftp = ftplib.FTP_TLS() if self._config["secure"] else ftplib.FTP()
            ftp.encoding = self.encoding
            try:
                ftp.connect(self._config["host"], self._config["port"])
                ftp.login(self._config["user"], self._config["passwd"])
                if self._config["secure"]:
                    ftp.prot_p()
                if self._config["active"]:
                    ftp.set_pasv(False)
                if self._config["path"] != "":
                    ftp.cwd(self._config["path"])
                self._connection = ftp
                return
            except ftplib.all_errors:
                raise FTPStorageException(
                    "Connection or login error using data %s" % repr(self._config)
                )

    def disconnect(self):
        self._connection.quit()
        self._connection = None

    def _mkremdirs(self, path):
        pwd = self._connection.pwd()
        path_splitted = path.split(os.path.sep)
        for path_part in path_splitted:
            try:
                self._connection.cwd(path_part)
            except ftplib.all_errors:
                try:
                    self._connection.mkd(path_part)
                    self._connection.cwd(path_part)
                except ftplib.all_errors:
                    raise FTPStorageException("Cannot create directory chain %s" % path)
        self._connection.cwd(pwd)

    def _put_file(self, name, content):
        # Connection must be open!
        try:
            self._mkremdirs(os.path.dirname(name))
            pwd = self._connection.pwd()
            self._connection.cwd(os.path.dirname(name))
            self._connection.storbinary(
                "STOR " + os.path.basename(name),
                content.file,
                content.DEFAULT_CHUNK_SIZE,
            )
            self._connection.cwd(pwd)
        except ftplib.all_errors:
            raise FTPStorageException("Error writing file %s" % name)

    def _open(self, name, mode="rb"):
        remote_file = FTPStorageFile(name, self, mode=mode)
        return remote_file

    def _read(self, name):
        memory_file = io.BytesIO()
        try:
            pwd = self._connection.pwd()
            self._connection.cwd(os.path.dirname(name))
            self._connection.retrbinary(
                "RETR " + os.path.basename(name), memory_file.write
            )
            self._connection.cwd(pwd)
            memory_file.seek(0)
            return memory_file
        except ftplib.all_errors:
            raise FTPStorageException("Error reading file %s" % name)

    def _save(self, name, content):
        content.open()
        self._start_connection()
        self._put_file(name, content)
        content.close()
        return name

    def _get_dir_details(self, path):
        # Connection must be open!
        try:
            lines = []
            self._connection.retrlines("LIST " + path, lines.append)
            dirs = {}
            files = {}
            for line in lines:
                words = line.split()
                if len(words) < 6:
                    continue
                if words[-2] == "->":
                    continue
                if words[0][0] == "d":
                    dirs[words[-1]] = 0
                elif words[0][0] == "-":
                    files[words[-1]] = int(words[-5])
            return dirs, files
        except ftplib.all_errors:
            raise FTPStorageException("Error getting listing for %s" % path)

    def listdir(self, path):
        self._start_connection()
        try:
            dirs, files = self._get_dir_details(path)
            return list(dirs.keys()), list(files.keys())
        except FTPStorageException:
            raise

    def delete(self, name):
        if not self.exists(name):
            return
        self._start_connection()
        try:
            self._connection.delete(name)
        except ftplib.all_errors:
            raise FTPStorageException("Error when removing %s" % name)

    def exists(self, name):
        if self.allow_overwrite:
            return False

        self._start_connection()
        try:
            nlst = self._connection.nlst(os.path.dirname(name) + "/")
            if name in nlst or os.path.basename(name) in nlst:
                return True
            else:
                return False
        except ftplib.error_temp:
            return False
        except ftplib.error_perm:
            # error_perm: 550 Can't find file
            return False
        except ftplib.all_errors:
            raise FTPStorageException("Error when testing existence of %s" % name)

    def size(self, name):
        self._start_connection()
        try:
            dirs, files = self._get_dir_details(os.path.dirname(name))
            if os.path.basename(name) in files:
                return files[os.path.basename(name)]
            else:
                return 0
        except FTPStorageException:
            return 0

    def url(self, name):
        if self.base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        return urllib.parse.urljoin(self.base_url, name).replace("\\", "/")


class FTPStorageFile(File):
    def __init__(self, name, storage, mode):
        self.name = name
        self._storage = storage
        self._mode = mode
        self._is_dirty = False
        self.file = io.BytesIO()
        self._is_read = False

    @property
    def size(self):
        if not hasattr(self, "_size"):
            self._size = self._storage.size(self.name)
        return self._size

    def readlines(self):
        if not self._is_read:
            self._storage._start_connection()
            self.file = self._storage._read(self.name)
            self._is_read = True
        return self.file.readlines()

    def read(self, num_bytes=None):
        if not self._is_read:
            self._storage._start_connection()
            self.file = self._storage._read(self.name)
            self._is_read = True
        return self.file.read(num_bytes)

    def write(self, content):
        if "w" not in self._mode:
            raise AttributeError("File was opened for read-only access.")
        self.file = io.BytesIO(content)
        self._is_dirty = True
        self._is_read = True

    def close(self):
        if self._is_dirty:
            self._storage._start_connection()
            self._storage._put_file(self.name, self)
            self._storage.disconnect()
        self.file.close()
