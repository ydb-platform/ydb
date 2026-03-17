"""OpenAPI spec validator handlers module."""
import contextlib

from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen
from yaml import load

from openapi_spec_validator.loaders import ExtendedSafeLoader


class FileObjectHandler(object):
    """OpenAPI spec validator file-like object handler."""

    def __init__(self, **options):
        self.options = options

    @property
    def loader(self):
        return self.options.get('loader', ExtendedSafeLoader)

    def __call__(self, f):
        return load(f, self.loader)


class UrlHandler(FileObjectHandler):
    """OpenAPI spec validator URL scheme handler."""

    def __init__(self, *allowed_schemes, **options):
        super(UrlHandler, self).__init__(**options)
        self.allowed_schemes = allowed_schemes

    def __call__(self, url, timeout=1):
        assert urlparse(url).scheme in self.allowed_schemes

        f = urlopen(url, timeout=timeout)
        with contextlib.closing(f) as fh:
            return super(UrlHandler, self).__call__(fh)
