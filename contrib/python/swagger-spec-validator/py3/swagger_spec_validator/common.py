from __future__ import annotations

import contextlib
import functools
import os
import sys
from functools import lru_cache
from typing import Any
from typing import Callable
from typing import TypeVar
from urllib.parse import urljoin
from urllib.request import pathname2url
from urllib.request import urlopen

import importlib.resources
import yaml
from typing_extensions import ParamSpec

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader  # type: ignore


TIMEOUT_SEC = 1.0
P = ParamSpec("P")
T = TypeVar("T")


def wrap_exception(method: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(method)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return method(*args, **kwargs)
        except Exception as e:
            raise SwaggerValidationError(str(e), e).with_traceback(sys.exc_info()[2])

    return wrapper


def get_uri_from_file_path(file_path: str) -> str:
    return urljoin("file://", pathname2url(os.path.abspath(file_path)))


def read_file(file_path: str) -> dict[str, Any]:
    """
    Utility method for reading a JSON/YAML file and converting it to a Python dictionary
    :param file_path: path of the file to read

    :return: Python dictionary representation of the JSON file
    :rtype: dict
    """
    return read_url(get_uri_from_file_path(file_path))


@lru_cache
def read_resource_file(resource_path: str) -> tuple[dict[str, Any], str]:
    ref = importlib.resources.files("swagger_spec_validator") / resource_path
    with importlib.resources.as_file(ref) as path:
        return read_file(path), path


def read_url(url: str, timeout: float = TIMEOUT_SEC) -> dict[str, Any]:
    with contextlib.closing(urlopen(url, timeout=timeout)) as fh:
        # NOTE: JSON is a subset of YAML so it is safe to read JSON as it is YAML
        return yaml.load(fh.read().decode("utf-8"), Loader=SafeLoader)


class SwaggerValidationError(Exception):
    """Exception raised in case of a validation error."""

    pass


class SwaggerValidationWarning(UserWarning):
    """Warning raised during validation."""

    pass
