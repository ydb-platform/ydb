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
import platform
import sys
from pathlib import Path
from collections.abc import Iterable, MutableMapping
from string import ascii_letters
from typing import Optional
from urllib.parse import urlsplit, urlunsplit, quote, quote_plus, unquote, unquote_plus

from xmlschema.aliases import NormalizedLocationsType, LocationsType
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.utils.paths import LocationPath


def is_local_scheme(scheme: str) -> bool:
    return not scheme or scheme == 'file' or scheme in ascii_letters and len(scheme) == 1


def get_uri(scheme: str = '', authority: str = '', path: str = '',
            query: str = '', fragment: str = '') -> str:
    """
    Get the URI from components, according to https://datatracker.ietf.org/doc/html/rfc3986.
    """
    if scheme == 'urn':
        if not path or authority or query or fragment:
            raise XMLSchemaValueError("An URN can have only scheme and path components")
        elif path.startswith(':') or path.endswith(':'):
            raise XMLSchemaValueError(f"Invalid URN path {path!r}")
        return 'urn:' + path

    if authority:
        if path and path[:1] != '/':
            path = '/' + path
        url = f'//{authority}{path}'
    elif scheme in ascii_letters and len(scheme) == 1:
        url = path
    elif scheme and path.startswith(('/', '\\')) or not scheme and path.startswith('//'):
        url = '//' + path
    else:
        url = path

    if scheme:
        url = f'{scheme}:{url}'
    if query:
        url = f'{url}?{query}'
    if fragment:
        url = f'{url}#{fragment}'

    return url


def is_url(obj: object) -> bool:
    """Returns `True` if the provided object is a URL, `False` otherwise."""
    if isinstance(obj, str):
        if '\n' in obj or obj.lstrip().startswith('<'):
            return False
    elif isinstance(obj, bytes):
        if b'\n' in obj or obj.lstrip().startswith(b'<'):
            return False
    else:
        return isinstance(obj, Path)

    try:
        urlsplit(obj.strip())
    except ValueError:  # pragma: no cover
        return False
    else:
        return True


def is_remote_url(obj: object) -> bool:
    if isinstance(obj, str):
        if '\n' in obj or obj.lstrip().startswith('<'):
            return False
        url = obj.strip()
    elif isinstance(obj, bytes):
        if b'\n' in obj or obj.lstrip().startswith(b'<'):
            return False
        url = obj.strip().decode('utf-8')
    else:
        return False

    try:
        return not is_local_scheme(urlsplit(url).scheme)
    except ValueError:  # pragma: no cover
        return False


def is_local_url(obj: object) -> bool:
    if isinstance(obj, str):
        if '\n' in obj or obj.lstrip().startswith('<'):
            return False
        url = obj.strip()
    elif isinstance(obj, bytes):
        if b'\n' in obj or obj.lstrip().startswith(b'<'):
            return False
        url = obj.strip().decode('utf-8')
    else:
        return isinstance(obj, Path)

    try:
        return is_local_scheme(urlsplit(url).scheme)
    except ValueError:  # pragma: no cover
        return False


def get_url(obj: object) -> Optional[str]:
    """If the argument is a URL returns it as a string, returns `None` otherwise."""
    if isinstance(obj, str):
        if '\n' in obj or obj.lstrip().startswith('<'):
            return None
        try:
            urlsplit(obj.strip()).geturl()
        except ValueError:  # pragma: no cover
            return None
        else:
            return obj

    elif isinstance(obj, bytes):
        if b'\n' in obj or obj.lstrip().startswith(b'<'):
            return None
        try:
            urlsplit(obj.strip()).geturl()
        except ValueError:  # pragma: no cover
            return None
        else:
            return obj.decode()

    elif isinstance(obj, Path):
        return str(obj)
    else:
        return None


def is_encoded_url(url: str) -> bool:
    """
    Determines whether the given URL is encoded. The case with '+' and without
    spaces is not univocal and the plus signs are ignored for the result.
    """
    return unquote(url) != url or \
        '+' in url and ' ' not in url and \
        unquote(url.replace('+', '$')) != url.replace('+', '$')


def is_safe_url(url: str, method: str = 'xml') -> bool:
    """Determines whether the given URL is safe."""
    query_quote = quote_plus if method == 'html' else quote
    query_unquote = unquote_plus if method == 'html' else unquote

    parts = urlsplit(url)
    path_safe = ':/\\' if is_local_scheme(parts.scheme) else '/'

    return parts.netloc == quote(unquote(parts.netloc), safe='@:') and \
        parts.path == quote(unquote(parts.path), safe=path_safe) and \
        parts.query == query_quote(query_unquote(parts.query), safe=';/?:@=&') and \
        parts.fragment == query_quote(query_unquote(parts.fragment), safe=';/?:@=&')


def encode_url(url: str, method: str = 'xml') -> str:
    """Encode the given url, if necessary."""
    if is_safe_url(url, method):
        return url
    elif is_encoded_url(url):
        url = decode_url(url, method)

    query_quote = quote_plus if method == 'html' else quote
    parts = urlsplit(url)
    path_safe = ':/\\' if is_local_scheme(parts.scheme) else '/'

    return urlunsplit((
        parts.scheme,
        quote(parts.netloc, safe='@:'),
        quote(parts.path, safe=path_safe),
        query_quote(parts.query, safe=';/?:@=&'),
        query_quote(parts.fragment, safe=';/?:@=&'),
    ))


def decode_url(url: str, method: str = 'xml') -> str:
    """Decode the given url, if necessary."""
    if not is_encoded_url(url):
        return url

    query_unquote = unquote_plus if method == 'html' else unquote

    parts = urlsplit(url)
    return urlunsplit((
        parts.scheme,
        unquote(parts.netloc),
        unquote(parts.path),
        query_unquote(parts.query),
        query_unquote(parts.fragment),
    ))


def normalize_url(url: str, base_url: Optional[str] = None,
                  keep_relative: bool = False, method: str = 'xml') -> str:
    """
    Returns a normalized URL eventually joining it to a base URL if it's a relative path.
    Path names are converted to 'file' scheme URLs and unsafe characters are encoded.
    Query and fragments parts are kept only for non-local URLs

    :param url: a relative or absolute URL.
    :param base_url: a reference base URL.
    :param keep_relative: if set to `True` keeps relative file paths, which would \
    not strictly conformant to specification (RFC 8089), because *urlopen()* doesn't \
    accept a simple pathname.
    :param method: method used to encode query and fragment parts. If set to `html` \
    the whitespaces are replaced with `+` characters.
    :return: a normalized URL string.
    """
    url = url.lstrip()
    url_parts = urlsplit(url)
    if not is_local_scheme(url_parts.scheme):
        return encode_url(get_uri(*url_parts), method)

    if url.startswith(('//', '\\\\')) and sys.version_info < (3, 12, 5):
        # workaround for UNC and Python 3.10/3.11
        path = LocationPath.from_uri(f'file://{url}')
        if url.startswith('////'):
            return 'file:///' + path.normalize().as_uri()[5:]
        elif url.startswith('///'):
            return '//' + path.normalize().as_uri()
        return path.normalize().as_uri()

    path = LocationPath.from_uri(url)
    if path.is_absolute():
        return path.normalize().as_uri()

    if base_url is not None:
        base_url = base_url.lstrip()

        if base_url.startswith(('//', '\\\\')) and sys.version_info < (3, 12, 5):
            # workaround for UNC and Python 3.10/3.11
            base_path = LocationPath.from_uri(f'file://{base_url}')
            if base_url.startswith('////'):
                return 'file:///' + base_path.joinpath(path).normalize().as_uri()[5:]
            elif base_url.startswith('///'):
                return '//' + base_path.joinpath(path).normalize().as_uri()
            return base_path.joinpath(path).normalize().as_uri()

        base_url_parts = urlsplit(base_url)
        base_path = LocationPath.from_uri(base_url)

        if is_local_scheme(base_url_parts.scheme):
            path = base_path.joinpath(path)
        elif not url_parts.scheme:
            url = get_uri(
                base_url_parts.scheme,
                base_url_parts.netloc,
                base_path.joinpath(path).normalize().as_posix(),
            )
            return encode_url(url, method)

    if path.is_absolute() or keep_relative:
        return path.normalize().as_uri()

    base_path = LocationPath(os.getcwd())
    return base_path.joinpath(path).normalize().as_uri()


def location_is_file(url: str) -> bool:
    if not is_local_url(url):
        return False
    if os.path.isfile(url):
        return True
    path = unquote(urlsplit(normalize_url(url)).path)
    if path.startswith('/') and platform.system() == 'Windows':
        path = path[1:]
    return os.path.isfile(path)


def normalize_locations(locations: LocationsType,
                        base_url: Optional[str] = None,
                        keep_relative: bool = False) -> NormalizedLocationsType:
    """
    Returns a list of normalized locations. The locations are normalized using
    the base URL of the instance.

    :param locations: a dictionary or a list of couples containing namespace location hints.
    :param base_url: the reference base URL for construct the normalized URL from the argument.
    :param keep_relative: if set to `True` keeps relative file paths, which would not strictly \
    conformant to URL format specification.
    :return: a list of couples containing normalized namespace location hints.
    """
    normalized_locations = []
    if isinstance(locations, MutableMapping):
        for ns, value in locations.items():
            if isinstance(value, list):
                normalized_locations.extend(
                    [(ns, normalize_url(url, base_url, keep_relative)) for url in value]
                )
            else:
                normalized_locations.append((ns, normalize_url(value, base_url, keep_relative)))
    else:
        normalized_locations.extend(
            [(ns, normalize_url(url, base_url, keep_relative)) for ns, url in locations]
        )
    return normalized_locations


def match_location(url: str, locations: Iterable[str]) -> Optional[str]:
    """
    Match a URL against a group of locations. Give priority to exact matches,
    then to the match with the highest score after filtering out the locations
    that are not compatible with provided url. The score of a location path is
    determined by the number of path levels minus the number of parent steps.
    If no match is found returns `None`.
    """
    def is_compatible(loc: str) -> bool:
        parts = urlsplit(loc)
        return not parts.scheme or scheme == parts.scheme and netloc == parts.netloc

    if url in locations:
        return url

    scheme, netloc = urlsplit(url)[:2]
    path = LocationPath.from_uri(url).normalize()
    matching_url = None
    matching_score = None

    for other_url in filter(is_compatible, locations):
        other_path = LocationPath.from_uri(other_url).normalize()
        pattern = other_path.as_posix().replace('..', '*')

        if path.match(pattern):
            score = pattern.count('/') - pattern.count('*')
            if matching_score is None or matching_score < score:
                matching_score = score
                matching_url = other_url

    return matching_url
