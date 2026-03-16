# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import functools
import os
import sys

import six
import yaml
from pkg_resources import resource_filename, resource_string
from six.moves.urllib.parse import urljoin
from six.moves.urllib.request import pathname2url
from six.moves.urllib.request import urlopen
try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader

try:
    from functools import lru_cache
except ImportError:  # pragma: no cover
    from functools32 import lru_cache


TIMEOUT_SEC = 1


def wrap_exception(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            six.reraise(
                SwaggerValidationError,
                SwaggerValidationError(str(e), e),
                sys.exc_info()[2])
    return wrapper


def get_uri_from_file_path(file_path):
    return urljoin('file://', pathname2url(os.path.abspath(file_path)))


def read_file(file_path):
    """
    Utility method for reading a JSON/YAML file and converting it to a Python dictionary
    :param file_path: path of the file to read

    :return: Python dictionary representation of the JSON file
    :rtype: dict
    """
    return read_url(get_uri_from_file_path(file_path))


@lru_cache()
def read_resource_file(resource_path):
    schema_path = resource_filename('swagger_spec_validator', resource_path)
    return yaml.load(resource_string('swagger_spec_validator', resource_path).decode('utf-8'), Loader=SafeLoader), schema_path


def read_url(url, timeout=TIMEOUT_SEC):
    with contextlib.closing(urlopen(url, timeout=timeout)) as fh:
        # NOTE: JSON is a subset of YAML so it is safe to read JSON as it is YAML
        return yaml.load(fh.read().decode('utf-8'), Loader=SafeLoader)


class SwaggerValidationError(Exception):
    """Exception raised in case of a validation error."""
    pass


class SwaggerValidationWarning(UserWarning):
    """Warning raised during validation."""
    pass
