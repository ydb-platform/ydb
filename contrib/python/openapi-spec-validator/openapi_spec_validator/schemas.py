"""OpenAIP spec validator schemas module."""
import os

from io import BytesIO

from pkgutil import get_data
from six.moves.urllib import parse, request
from yaml import load

from openapi_spec_validator.loaders import ExtendedSafeLoader


def get_openapi_schema(version):
    # XXX: PATCH adaptation schema loading to Arcadia
    path = 'resources/schemas/v{0}/schema.json'.format(version)
    data = get_data('openapi_spec_validator', path)
    schema = load(BytesIO(data), ExtendedSafeLoader)
    schema_url = parse.urljoin('file:', request.pathname2url(path))
    return schema, schema_url


def read_yaml_file(path, loader=ExtendedSafeLoader):
    """Open a file, read it and return its contents."""
    with open(path) as fh:
        return load(fh, loader)
