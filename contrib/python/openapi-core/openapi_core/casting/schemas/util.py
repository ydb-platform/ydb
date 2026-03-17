"""OpenAPI core casting schemas util module"""
from distutils.util import strtobool
from six import string_types


def forcebool(val):
    if isinstance(val, string_types):
        val = strtobool(val)

    return bool(val)
