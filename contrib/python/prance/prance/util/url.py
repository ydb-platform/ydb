"""This submodule contains code for fetching/parsing URLs."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


from urllib import parse


class ResolutionError(LookupError):
    pass


def urlresource(url):
    """
    Return the resource part of a parsed URL.

    The resource part is defined as the part without query, parameters or
    fragment. Just the scheme, netloc and path remains.

    :param tuple url: A parsed URL
    :return: The resource part of the URL
    :rtype: str
    """
    res_list = list(url)[0:3] + [None, None, None]
    return parse.ParseResult(*res_list).geturl()


def absurl(url, relative_to=None):
    """
    Turn relative file URLs into absolute file URLs.

    This is necessary, because while JSON pointers do not allow relative file
    URLs, Swagger/OpenAPI explicitly does. We need to make relative paths
    absolute before passing them off to jsonschema for verification.

    Non-file URLs are left untouched. URLs without scheme are assumed to be file
    URLs.

    :param str/tuple url: The input URL.
    :param str/tuple relative_to: [optional] The URL to which the input URL is
        relative.
    :return: The output URL, parsed into components.
    :rtype: tuple
    """
    # Parse input URL, if necessary
    parsed = url
    if not isinstance(parsed, tuple):
        from .fs import is_pathname_valid

        if is_pathname_valid(url):
            from . import fs

            url = fs.to_posix(url)
        try:
            parsed = parse.urlparse(url)
        except Exception as ex:
            from .exceptions import raise_from

            raise_from(ResolutionError, ex, f"Unable to parse url: {url}")

    # Any non-file scheme we just return immediately.
    if parsed.scheme not in (None, "", "file"):
        return parsed

    # Parse up the reference URL
    reference = relative_to
    if reference and not isinstance(reference, tuple):
        from .fs import is_pathname_valid

        if is_pathname_valid(reference):
            from . import fs

            reference = fs.to_posix(reference)
        reference = parse.urlparse(reference)

    # If the input URL has no path, we assume only its fragment matters.
    # That is, we'll have to set the fragment of the reference URL to that
    # of the input URL, and return the result.
    import os.path
    from .fs import from_posix, abspath

    result_list = None
    if not parsed.path:
        if not reference or not reference.path:
            raise ResolutionError(
                "Cannot build an absolute file URL from a fragment"
                " without a reference with path!"
            )
        result_list = list(reference)
        result_list[5] = parsed.fragment
    elif os.path.isabs(from_posix(parsed.path)):
        # We have an absolute path, so we can ignore the reference entirely!
        result_list = list(parsed)
        result_list[0] = "file"  # in case it was empty
    else:
        # If we have a relative path, we require a reference.
        if not reference:
            raise ResolutionError(
                "Cannot build an absolute file URL from a relative"
                " path without a reference!"
            )
        if reference.scheme not in (None, "", "file"):
            raise ResolutionError(
                "Cannot build an absolute file URL with a non-file" " reference!"
            )

        result_list = list(parsed)
        result_list[0] = "file"  # in case it was empty
        result_list[2] = abspath(from_posix(parsed.path), from_posix(reference.path))

    # Reassemble the result and return it
    result = parse.ParseResult(*result_list)
    return result


def split_url_reference(base_url, reference):
    """
    Return a normalized, parsed URL and object path.

    The reference string is a JSON reference, i.e. a URL with a fragment that
    contains an object path into the referenced resource.

    The base URL is used as a reference point for relative references.

    :param mixed base_url: A parsed URL.
    :param str reference: A JSON reference string.
    :return: The parsed absolute URL of the reference and the object path.
    """
    # Parse URL
    parsed_url = absurl(reference, base_url)

    # Grab object path
    obj_path = parsed_url.fragment.split("/")
    while len(obj_path) and not obj_path[0]:
        obj_path = obj_path[1:]

    # Normalize the object path by substituting ~1 and ~0 respectively.
    def _normalize(path):
        path = path.replace("~1", "/")
        path = path.replace("~0", "~")
        return path

    obj_path = [_normalize(p) for p in obj_path]

    return parsed_url, obj_path


def fetch_url_text(url, cache={}, encoding=None):
    """
    Fetch the URL.

    If the URL is a file URL, the format used for parsing depends on the file
    extension. Otherwise, YAML is assumed.

    The URL may also use the `python` scheme. In this scheme, the netloc part
    refers to an importable python package, and the path part to a path relative
    to the package path, e.g. `python://some_package/path/to/file.yaml`.

    :param tuple url: The url, parsed as returned by `absurl` above.
    :param Mapping cache: An optional cache. If the URL can be found in the
      cache, return the cache contents.
    :param str encoding: Provide an encoding for local URLs to override
      encoding detection, if desired. Defaults to None.
    :return: The resource text of the URL, and the content type.
    :rtype: tuple
    """
    url_key = "text_" + urlresource(url)
    entry = cache.get(url_key, None)
    if entry is not None:
        return entry

    # Fetch contents according to scheme. We assume requests can handle all the
    # non-file schemes, or throw otherwise.
    content = None
    content_type = None
    if url.scheme in (None, "", "file"):
        from .fs import read_file, from_posix

        try:
            content = read_file(from_posix(url.path), encoding)
        except FileNotFoundError as ex:
            from .exceptions import raise_from

            raise_from(ResolutionError, ex, f"File not found: {url.path}")
    elif url.scheme == "python":
        # Resolve package path
        package = url.netloc
        path = url.path
        if path[0] == "/":
            path = path[1:]

        import pkg_resources

        content = pkg_resources.resource_string(package, path).decode(encoding or 'utf-8')
    else:
        import requests

        response = requests.get(url.geturl())
        if not response.ok:  # pragma: nocover
            raise ResolutionError(
                'Cannot fetch URL "%s": %d %s'
                % (url.geturl(), response.status_code, response.reason)
            )
        content_type = response.headers.get("content-type", "text/plain")
        content = response.text

    cache[url_key] = (content, content_type)
    return content, content_type


def fetch_url(url, cache={}, encoding=None, strict=True):
    """
    Fetch the URL and parse the contents.

    Same as fetch_url_text(), but also parses the content and only
    returns the parse results.

    :param tuple url: The url, parsed as returned by `absurl` above.
    :param Mapping cache: An optional cache. If the URL can be found in the
      cache, return the cache contents.
    :param str encoding: Provide an encoding for local URLs to override
      encoding detection, if desired. Defaults to None.
    :return: The parsed file.
    :rtype: dict
    """
    # Return from cache, if parsed result is already present.
    url_key = (urlresource(url), strict)
    entry = cache.get(url_key, None)
    if entry is not None:
        return entry.copy()

    # Fetch URL text
    content, content_type = fetch_url_text(url, cache, encoding=encoding)

    # Parse the result
    from .formats import parse_spec

    result = parse_spec(content, url.path, content_type=content_type)

    # Perform some sanitization in lenient mode.
    if not strict:
        from . import stringify_keys

        result = stringify_keys(result)

    # Cache and return result
    cache[url_key] = result
    return result.copy()
