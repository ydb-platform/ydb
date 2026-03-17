import os
import pathlib
import posixpath

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files.utils import FileProxyMixin
from django.utils.encoding import force_bytes


def to_bytes(content):
    """Wrap Django's force_bytes to pass through bytearrays."""
    if isinstance(content, bytearray):
        return content

    return force_bytes(content)


def setting(name, default=None):
    """
    Helper function to get a Django setting by name. If setting doesn't exists
    it will return a default.

    :param name: Name of setting
    :type name: str
    :param default: Value if setting is unfound
    :returns: Setting's value
    """
    return getattr(settings, name, default)


def clean_name(name):
    """
    Normalize the name.

    Includes cleaning up Windows style paths, ensuring an ending trailing slash,
    and coercing from pathlib.PurePath.
    """
    if isinstance(name, pathlib.PurePath):
        name = str(name)

    # Normalize Windows style paths
    clean_name = posixpath.normpath(name).replace("\\", "/")

    # os.path.normpath() can strip trailing slashes so we implement
    # a workaround here.
    if name.endswith("/") and not clean_name.endswith("/"):
        # Add a trailing slash as it was stripped.
        clean_name += "/"

    # Given an empty string, os.path.normpath() will return ., which we don't want
    if clean_name == ".":
        clean_name = ""

    return clean_name


def safe_join(base, *paths):
    """
    A version of django.utils._os.safe_join for S3 paths.

    Joins one or more path components to the base path component
    intelligently. Returns a normalized version of the final path.

    The final path must be located inside of the base path component
    (otherwise a ValueError is raised).

    Paths outside the base path indicate a possible security
    sensitive operation.
    """
    base_path = base
    base_path = base_path.rstrip("/")
    paths = list(paths)

    final_path = base_path + "/"
    for path in paths:
        _final_path = posixpath.normpath(posixpath.join(final_path, path))
        # posixpath.normpath() strips the trailing /. Add it back.
        if path.endswith("/") or _final_path + "/" == final_path:
            _final_path += "/"
        final_path = _final_path
    if final_path == base_path:
        final_path += "/"

    # Ensure final_path starts with base_path and that the next character after
    # the base path is /.
    base_path_len = len(base_path)
    if not final_path.startswith(base_path) or final_path[base_path_len] != "/":
        raise ValueError(
            "the joined path is located outside of the base path component"
        )

    return final_path.lstrip("/")


def check_location(storage):
    if storage.location.startswith("/"):
        correct = storage.location.lstrip("/")
        raise ImproperlyConfigured(
            (
                "{}.location cannot begin with a leading slash. Found '{}'. Use '{}' "
                "instead."
            ).format(
                storage.__class__.__name__,
                storage.location,
                correct,
            )
        )


def lookup_env(names):
    """
    Look up for names in environment. Returns the first element
    found.
    """
    for name in names:
        value = os.environ.get(name)
        if value:
            return value


def is_seekable(file_object):
    return not hasattr(file_object, "seekable") or file_object.seekable()


class ReadBytesWrapper(FileProxyMixin):
    """
    A wrapper for a file-like object, that makes read() always returns bytes.
    """

    def __init__(self, file, encoding=None):
        """
        :param file: The file-like object to wrap.
        :param encoding: Specify the encoding to use when file.read() returns strings.
            If not provided will default to file.encoding, of if that's not available,
            to utf-8.
        """
        self.file = file
        self._encoding = encoding or getattr(file, "encoding", None) or "utf-8"

    def read(self, *args, **kwargs):
        content = self.file.read(*args, **kwargs)

        if not isinstance(content, bytes):
            content = content.encode(self._encoding)
        return content

    def close(self):
        self.file.close()

    def readable(self):
        return True
