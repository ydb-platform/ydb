#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import os.path
import ntpath
import platform
import posixpath
import sys
from pathlib import PurePath, PurePosixPath, PureWindowsPath
from string import ascii_letters
from urllib.parse import urlsplit, unquote, quote_from_bytes

from xmlschema.exceptions import XMLSchemaValueError

SUPPORTS_UNC_DRIVE = sys.version_info >= (3, 12, 5)


def is_unc_path(path: str) -> bool:
    """
    Returns `True` if the provided path is a UNC path, `False` otherwise.
    Based on the capabilities of `PureWindowsPath` of the Python release.
    """
    return LocationWindowsPath(path).drive.startswith('\\\\')


def is_drive_path(path: str) -> bool:
    """Returns `True` if the provided path starts with a drive (e.g. 'C:'), `False` otherwise."""
    drive = ntpath.splitdrive(path)[0]
    return len(drive) == 2 and drive[1] == ':' and drive[0] in ascii_letters


class LocationPath(PurePath):
    """
    A version of pathlib.PurePath with an enhanced URI conversion and for
    the normalization of location paths.

    A system independent path normalization without resolution is essential for
    processing resource locations, so the use or base class internals can be
    necessary for using pathlib. Despite the URL path has to be considered
    case-sensitive (ref. https://www.w3.org/TR/WD-html40-970708/htmlweb.html)
    this not always happen. On the other hand the initial source is often a
    filepath, so the better choice is to maintain location paths still related
    to the operating system.
    """
    _path_module = os.path
    __slots__ = ()

    def __new__(cls, *args: str) -> 'LocationPath':
        if cls is LocationPath:
            cls = LocationWindowsPath if os.name == 'nt' else LocationPosixPath
        return super().__new__(cls, *args)  # type: ignore[arg-type, unused-ignore]

    @classmethod
    def from_uri(cls, uri: str) -> 'LocationPath':
        """
        Parse a URI and return a LocationPath. For non-local schemes like 'http',
        'https', etc. a LocationPosixPath is returned. For Windows related file
        paths, like a path with a drive, a UNC path or a path containing a backslash,
        a LocationWindowsPath is returned.
        """
        uri = uri.strip()
        parts = urlsplit(uri)

        if (scheme := parts.scheme) == 'urn':
            raise XMLSchemaValueError(f"Can't create a {cls!r} from an URN!")
        elif scheme and scheme != 'file' and (scheme not in ascii_letters or len(scheme) != 1):
            return LocationPosixPath(unquote(parts.path))

        path = parts.path
        if parts.netloc:
            if path and path[:1] != '/':
                path = '/' + path
            if parts.netloc.startswith(('/', '\\')):
                path = parts.netloc + path
            else:
                path = f'//{parts.netloc}{path}'

        elif not parts.scheme and path.startswith('//'):
            path = '//' + path

        if parts.query:
            path = f'{path}?{parts.query}'
        if parts.fragment:
            path = f'{path}#{parts.fragment}'

        if parts.scheme in ascii_letters and len(parts.scheme) == 1:
            # uri is a Windows path with a drive, e.g. k:/Python/lib/file
            path = f'{uri[0]}:{path}'  # urlsplit() converts scheme to lowercase
            return LocationWindowsPath(unquote(path))

        # Detect invalid Windows paths (rooted or UNC path followed by a drive)
        for k in range(len(path)):
            if path[k] not in '/\\':
                if not k or not is_drive_path(path[k:]):
                    break
                elif k == 1 and parts.scheme == 'file':
                    # Valid case for a URL with a file scheme
                    return LocationWindowsPath(unquote(path[1:]))
                else:
                    raise XMLSchemaValueError(f"Invalid URI {uri!r}")

        if '\\' in path or platform.system() == 'Windows':
            return LocationWindowsPath(unquote(path))
        elif ntpath.splitdrive(path)[0]:
            location_path = LocationWindowsPath(unquote(path))
            if location_path.drive:
                # PureWindowsPath not detects a drive in Python 3.11.x also
                # if it's detected by ntpath.splitdrive().
                return location_path

        return LocationPosixPath(unquote(path))

    def as_uri(self) -> str:
        # Implementation that maps relative paths to not RFC 8089 compliant relative
        # file URIs because urlopen() doesn't accept simple paths. For UNC paths uses
        # the format with four slashes to let urlopen() works.

        drive = self.drive
        if len(drive) == 2 and drive[1] == ':' and drive[0] in ascii_letters:
            # A Windows path with a drive: 'c:\dir\file' => 'file:///c:/dir/file'
            prefix = 'file:///' + drive
            path = self.as_posix()[2:]
        elif drive:
            # UNC format case: '\\host\dir\file' => 'file:////host/dir/file'
            prefix = 'file://'
            path = self.as_posix()
        else:
            path = self.as_posix()
            if path.startswith('/'):
                # A Windows relative path or an absolute posix path:
                #  ('\dir\file' | '/dir/file') => 'file://dir/file'
                prefix = 'file://'
            else:
                # A relative posix path: 'dir/file' => 'file:dir/file'
                prefix = 'file:'

        return prefix + quote_from_bytes(os.fsencode(path))

    def normalize(self) -> 'LocationPath':
        normalized_path = self._path_module.normpath(str(self))
        return self.__class__(normalized_path)


class LocationPosixPath(LocationPath, PurePosixPath):
    _path_module = posixpath
    __slots__ = ()


class LocationWindowsPath(LocationPath, PureWindowsPath):
    _path_module = ntpath
    __slots__ = ()
