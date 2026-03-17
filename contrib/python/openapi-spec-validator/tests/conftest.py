import pytest
from six.moves.urllib import request
from six.moves.urllib.parse import urlunparse
from yaml import safe_load

from openapi_spec_validator import (openapi_v3_spec_validator,
                                    openapi_v2_spec_validator)
from openapi_spec_validator.schemas import read_yaml_file
from pytest_localserver.plugin import httpserver  # noqa


def spec_url(spec_file, schema='file'):
    return urlunparse((schema, None, spec_file, None, None, None))


def spec_from_file(spec_file):
    # XXX: PATCH changes that makes tests work in Arcadia
    return read_yaml_file(spec_file)


def spec_from_url(spec_url):
    content = request.urlopen(spec_url)
    return safe_load(content)


class Factory(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


@pytest.fixture
def factory():
    return Factory(
        spec_url=spec_url,
        spec_from_file=spec_from_file,
        spec_from_url=spec_from_url,
    )


@pytest.fixture
def validator():
    return openapi_v3_spec_validator


@pytest.fixture
def swagger_validator():
    return openapi_v2_spec_validator
