"""Implementation of Apache VFS schemes and URLs."""

import sys
import re
from urllib.parse import urlparse


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
    'gs': 'gs',
}

CURLSCHEMES = {k for k, v in SCHEMES.items() if v == 'curl'}

# TODO: extend for other cloud platforms.
REMOTESCHEMES = {k for k, v in SCHEMES.items() if v in ('curl', 's3', 'gs')}


def valid_vsi(vsi):
    """Ensures all parts of our vsi path are valid schemes."""
    return all(p in SCHEMES for p in vsi.split('+'))


def is_remote(scheme):
    if scheme is None:
        return False
    return any(p in REMOTESCHEMES for p in scheme.split('+'))


def vsi_path(path, vsi=None, archive=None):
    # If a VSI and archive file are specified, we convert the path to
    # an OGR VSI path (see cpl_vsi.h).
    if vsi:
        prefix = '/'.join(f'vsi{SCHEMES[p]}' for p in vsi.split('+'))
        if archive:
            result = f'/{prefix}/{archive}{path}'
        else:
            result = f'/{prefix}/{path}'
    else:
        result = path

    return result


def parse_paths(uri, vfs=None):
    """Parse a URI or Apache VFS URL into its parts

    Returns: tuple
        (path, scheme, archive)
    """
    archive = scheme = None
    path = uri
    # Windows drive letters (e.g. "C:\") confuse `urlparse` as they look like
    # URL schemes
    if sys.platform == "win32" and re.match("^[a-zA-Z]\\:", path):
        return path, None, None
    if vfs:
        parts = urlparse(vfs)
        scheme = parts.scheme
        archive = parts.path
        if parts.netloc and parts.netloc != 'localhost':
            archive = parts.netloc + archive
    else:
        parts = urlparse(path)
        scheme = parts.scheme
        path = parts.path
        if parts.netloc and parts.netloc != 'localhost':
            if scheme.split("+")[-1] in CURLSCHEMES:
                # We need to deal with cases such as zip+https://server.com/data.zip
                path = f"{scheme.split('+')[-1]}://{parts.netloc}{path}"
            else:
                path = parts.netloc + path
        if scheme in SCHEMES:
            parts = path.split('!')
            path = parts.pop() if parts else None
            archive = parts.pop() if parts else None

    scheme = None if not scheme else scheme
    return path, scheme, archive
