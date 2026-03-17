"""Utility functions."""

import re
import sys
from packaging.version import Version
from pathlib import Path
from urllib.parse import urlparse

from pyogrio._ogr import MULTI_EXTENSIONS
from pyogrio._vsi import vsimem_rmtree_toplevel as _vsimem_rmtree_toplevel


def get_vsi_path_or_buffer(path_or_buffer):
    """Get VSI-prefixed path or bytes buffer depending on type of path_or_buffer.

    If path_or_buffer is a bytes object, it will be returned directly and will
    be read into an in-memory dataset when passed to one of the Cython functions.

    If path_or_buffer is a file-like object with a read method, bytes will be
    read from the file-like object and returned.

    Otherwise, it will be converted to a string, and parsed to prefix with
    appropriate GDAL /vsi*/ prefixes.

    Parameters
    ----------
    path_or_buffer : str, pathlib.Path, bytes, or file-like
        A dataset path or URI, raw buffer, or file-like object with a read method.

    Returns
    -------
    str or bytes

    """
    # treat Path objects here already to ignore their read method + to avoid backslashes
    # on Windows.
    if isinstance(path_or_buffer, Path):
        return vsi_path(path_or_buffer)

    if isinstance(path_or_buffer, bytes):
        return path_or_buffer

    if hasattr(path_or_buffer, "read"):
        bytes_buffer = path_or_buffer.read()

        # rewind buffer if possible so that subsequent operations do not need to rewind
        if hasattr(path_or_buffer, "seekable") and path_or_buffer.seekable():
            path_or_buffer.seek(0)

        return bytes_buffer

    return vsi_path(str(path_or_buffer))


def vsi_path(path: str | Path) -> str:
    """Ensure path is a local path or a GDAL-compatible VSI path."""
    # Convert Path objects to string, but for VSI paths, keep posix style path.
    if isinstance(path, Path):
        if sys.platform == "win32" and path.as_posix().startswith("/vsi"):
            path = path.as_posix()
        else:
            path = str(path)

    # path is already in GDAL format
    if path.startswith("/vsi"):
        return path

    # Windows drive letters (e.g. "C:\") confuse `urlparse` as they look like
    # URL schemes
    if sys.platform == "win32" and re.match("^[a-zA-Z]\\:", path):
        # If it is not a zip file or it is multi-extension zip file that is directly
        # supported by a GDAL driver, return the path as is.
        if not path.split("!")[0].endswith(".zip"):
            return path
        if path.split("!")[0].endswith(MULTI_EXTENSIONS):
            return path

        # prefix then allow to proceed with remaining parsing
        path = f"zip://{path}"

    path, archive, scheme = _parse_uri(path)

    if (
        scheme
        or archive
        or (path.endswith(".zip") and not path.endswith(MULTI_EXTENSIONS))
    ):
        return _construct_vsi_path(path, archive, scheme)

    return path


# Supported URI schemes and their mapping to GDAL's VSI suffix.
SCHEMES = {
    "file": "file",
    "zip": "zip",
    "tar": "tar",
    "gzip": "gzip",
    "http": "curl",
    "https": "curl",
    "ftp": "curl",
    "s3": "s3",
    "gs": "gs",
    "az": "az",
    "adls": "adls",
    "adl": "adls",  # fsspec uses this
    "hdfs": "hdfs",
    "webhdfs": "webhdfs",
    # GDAL additionally supports oss and swift for remote filesystems, but
    # those are for now not added as supported URI
}

CURLSCHEMES = {k for k, v in SCHEMES.items() if v == "curl"}


def _parse_uri(path: str):
    """Parse a URI.

    Returns a tuples of (path, archive, scheme)

    path : str
        Parsed path. Includes the hostname and query string in the case
        of a URI.
    archive : str
        Parsed archive path.
    scheme : str
        URI scheme such as "https" or "zip+s3".
    """
    parts = urlparse(path, allow_fragments=False)

    # if the scheme is not one of GDAL's supported schemes, return raw path
    if parts.scheme and not all(p in SCHEMES for p in parts.scheme.split("+")):
        return path, "", ""

    # we have a URI
    path = parts.path
    scheme = parts.scheme or ""

    if parts.query:
        path += "?" + parts.query

    if parts.scheme and parts.netloc:
        path = parts.netloc + path

    parts = path.split("!")
    path = parts.pop() if parts else ""
    archive = parts.pop() if parts else ""
    return (path, archive, scheme)


def _construct_vsi_path(path, archive, scheme) -> str:
    """Convert a parsed path to a GDAL VSI path."""
    prefix = ""
    suffix = ""
    schemes = scheme.split("+")

    if "zip" not in schemes and (
        archive.endswith(".zip")
        or (path.endswith(".zip") and not path.endswith(MULTI_EXTENSIONS))
    ):
        schemes.insert(0, "zip")

    if schemes:
        prefix = "/".join(f"vsi{SCHEMES[p]}" for p in schemes if p and p != "file")

        if schemes[-1] in CURLSCHEMES:
            suffix = f"{schemes[-1]}://"

    if prefix:
        if archive:
            return "/{}/{}{}/{}".format(prefix, suffix, archive, path.lstrip("/"))
        else:
            return f"/{prefix}/{suffix}{path}"

    return path


def _preprocess_options_key_value(options):
    """Preprocess options.

    For example, `spatial_index=True` gets converted to `SPATIAL_INDEX="YES"`.
    """
    if not isinstance(options, dict):
        raise TypeError(f"Expected options to be a dict, got {type(options)}")

    result = {}
    for k, v in options.items():
        if v is None:
            continue
        k = k.upper()
        if isinstance(v, bool):
            v = "ON" if v else "OFF"
        else:
            v = str(v)
        result[k] = v
    return result


def _mask_to_wkb(mask):
    """Convert a Shapely mask geometry to WKB.

    Parameters
    ----------
    mask : Shapely geometry
        The geometry to convert to WKB.

    Returns
    -------
    WKB bytes or None

    Raises
    ------
    ValueError
        raised if Shapely >= 2.0 is not available or mask is not a Shapely
        Geometry object

    """
    if mask is None:
        return mask

    try:
        import shapely

        if Version(shapely.__version__) < Version("2.0.0"):
            shapely = None
    except ImportError:
        shapely = None

    if not shapely:
        raise ValueError("'mask' parameter requires Shapely >= 2.0")

    if not isinstance(mask, shapely.Geometry):
        raise ValueError("'mask' parameter must be a Shapely geometry")

    return shapely.to_wkb(mask)


def vsimem_rmtree_toplevel(path: str | Path):
    """Remove the parent directory of the file path recursively.

    This is used for final cleanup of an in-memory dataset, which may have been
    created within a directory to contain sibling files.

    Additional VSI handlers may be chained to the left of /vsimem/ in path and
    will be ignored.

    Remark: function is defined here to be able to run tests on it.

    Parameters
    ----------
    path : str or pathlib.Path
        path to in-memory file

    """
    if isinstance(path, Path):
        path = path.as_posix()

    _vsimem_rmtree_toplevel(path)
