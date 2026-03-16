"""Dataset paths, identifiers, and filenames

Note: this module is not part of Rasterio's API. It is for internal use
only.

"""

import os
import pathlib
import re
import sys
from urllib.parse import urlparse

import attr

from rasterio.errors import PathError

# Supported URI schemes and their mapping to GDAL's VSI suffix.
# TODO: extend for other cloud platforms.
SCHEMES = {
    'ftp': 'curl',
    'gzip': 'gzip',
    'http': 'curl',
    'https': 'curl',
    's3': 's3',
    'tar': 'tar',
    'zip': 'zip',
    'file': 'file',
    'oss': 'oss',
    'gs': 'gs',
    'az': 'az',
}

ARCHIVESCHEMES = set
CURLSCHEMES = {k for k, v in SCHEMES.items() if v == "curl"}

# TODO: extend for other cloud platforms.
REMOTESCHEMES = {
    k
    for k, v in SCHEMES.items()
    if v
    in (
        "curl",
        "s3",
        "oss",
        "gs",
        "az",
    )
}


class _Path:
    """Base class for dataset paths"""

    def as_vsi(self):
        return _vsi_path(self)


@attr.s(slots=True)
class _ParsedPath(_Path):
    """Result of parsing a dataset URI/Path

    Attributes
    ----------
    path : str
        Parsed path. Includes the hostname and query string in the case
        of a URI.
    archive : str
        Parsed archive path.
    scheme : str
        URI scheme such as "https" or "zip+s3".
    """
    path = attr.ib()
    archive = attr.ib()
    scheme = attr.ib()

    @classmethod
    def from_uri(cls, uri):
        parts = urlparse(uri)
        if sys.platform == "win32" and re.match(r"^[a-zA-Z]\:", parts.netloc):
            parsed_path = f"{parts.netloc}{parts.path}"
            parsed_netloc = None
        else:
            parsed_path = parts.path
            parsed_netloc = parts.netloc

        path = parsed_path
        scheme = parts.scheme or None

        if parts.query:
            path += "?" + parts.query

        if scheme and scheme.startswith(("gzip", "tar", "zip")):
            path_parts = path.split('!')
            path = path_parts.pop() if path_parts else None
            archive = path_parts.pop() if path_parts else None
        else:
            archive = None

        if scheme and parsed_netloc:
            if archive:
                archive = parsed_netloc + archive
            else:
                path = parsed_netloc + path

        return _ParsedPath(path, archive, scheme)

    @property
    def name(self):
        """The parsed path's original URI"""
        if not self.scheme:
            return self.path
        elif self.archive:
            return f"{self.scheme}://{self.archive}!{self.path}"
        else:
            return f"{self.scheme}://{self.path}"

    @property
    def is_remote(self):
        """Test if the path is a remote, network URI"""
        return bool(self.scheme) and self.scheme.split("+")[-1] in REMOTESCHEMES

    @property
    def is_local(self):
        """Test if the path is a local URI"""
        return not self.scheme or (self.scheme and self.scheme.split('+')[-1] not in REMOTESCHEMES)


@attr.s(slots=True)
class _UnparsedPath(_Path):
    """Encapsulates legacy GDAL filenames

    Attributes
    ----------
    path : str
        The legacy GDAL filename.
    """
    path = attr.ib()

    @property
    def name(self):
        """The unparsed path's original path"""
        return self.path


def _parse_path(path):
    """Parse a dataset's identifier or path into its parts

    Parameters
    ----------
    path : str or path-like object
        The path to be parsed.

    Returns
    -------
    ParsedPath or UnparsedPath

    Notes
    -----
    When legacy GDAL filenames are encountered, they will be returned
    in a UnparsedPath.

    """
    if isinstance(path, _Path):
        return path
    elif isinstance(path, pathlib.PurePath):
        return _ParsedPath(os.fspath(path), None, None)
    elif isinstance(path, str):
        if sys.platform == "win32" and re.match(r"^[a-zA-Z]\:", path):
            return _ParsedPath(path, None, None)
        elif path.startswith('/vsi'):
            return _UnparsedPath(path)
        else:
            parts = urlparse(path)
    else:
        raise PathError(f"invalid path '{path!r}'")

    # if the scheme is not one of Rasterio's supported schemes, we
    # return an UnparsedPath.
    if parts.scheme:
        if all(p in SCHEMES for p in parts.scheme.split('+')):
            return _ParsedPath.from_uri(path)

    return _UnparsedPath(path)


def _vsi_path(path):
    """Convert a parsed path to a GDAL VSI path

    Parameters
    ----------
    path : Path
        A ParsedPath or UnparsedPath object.

    Returns
    -------
    str

    """
    if isinstance(path, _UnparsedPath):
        return path.path

    elif isinstance(path, _ParsedPath):

        if not path.scheme:
            return path.path

        else:
            if path.scheme.split('+')[-1] in CURLSCHEMES:
                suffix = '{}://'.format(path.scheme.split('+')[-1])
            else:
                suffix = ''

            prefix = "/".join(
                f"vsi{SCHEMES[p]}" for p in path.scheme.split("+") if p != "file"
            )

            if prefix:
                if path.archive:
                    result = '/{}/{}{}/{}'.format(prefix, suffix, path.archive, path.path.lstrip('/'))
                else:
                    result = f"/{prefix}/{suffix}{path.path}"
            else:
                result = path.path
            return result

    else:
        raise ValueError("path must be a ParsedPath or UnparsedPath object")
