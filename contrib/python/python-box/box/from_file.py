#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from collections.abc import Callable
from json import JSONDecodeError
from os import PathLike
from pathlib import Path

from box.box import Box
from box.box_list import BoxList
from box.converters import msgpack_available, toon_available, toml_read_library, yaml_available, toml_decode_error
from box.exceptions import BoxError

try:
    from ruamel.yaml import YAMLError
except ImportError:
    try:
        from yaml import YAMLError  # type: ignore
    except ImportError:
        YAMLError = False  # type: ignore

try:
    from msgpack import UnpackException  # type: ignore
except ImportError:
    UnpackException = False  # type: ignore

try:
    from toon_format import ToonDecodeError  # type: ignore
except ImportError:
    ToonDecodeError = False  # type: ignore


__all__ = ["box_from_file", "box_from_string"]


def _to_json(file, encoding, errors, **kwargs):
    try:
        return Box.from_json(filename=file, encoding=encoding, errors=errors, **kwargs)
    except JSONDecodeError:
        raise BoxError("File is not JSON as expected")
    except BoxError:
        return BoxList.from_json(filename=file, encoding=encoding, errors=errors, **kwargs)


def _to_csv(file, encoding, errors, **kwargs):
    return BoxList.from_csv(filename=file, encoding=encoding, errors=errors, **kwargs)


def _to_yaml(file, encoding, errors, **kwargs):
    if not yaml_available:
        raise BoxError(
            f'File "{file}" is yaml but no package is available to open it. Please install "ruamel.yaml" or "PyYAML"'
        )
    try:
        return Box.from_yaml(filename=file, encoding=encoding, errors=errors, **kwargs)
    except YAMLError:
        raise BoxError("File is not YAML as expected")
    except BoxError:
        return BoxList.from_yaml(filename=file, encoding=encoding, errors=errors, **kwargs)


def _to_toml(file, encoding, errors, **kwargs):
    if not toml_read_library:
        raise BoxError(f'File "{file}" is toml but no package is available to open it. Please install "tomli"')
    try:
        return Box.from_toml(filename=file, encoding=encoding, errors=errors, **kwargs)
    except toml_decode_error:
        raise BoxError("File is not TOML as expected")


def _to_msgpack(file, _, __, **kwargs):
    if not msgpack_available:
        raise BoxError(f'File "{file}" is msgpack but no package is available to open it. Please install "msgpack"')
    try:
        return Box.from_msgpack(filename=file, **kwargs)
    except (UnpackException, ValueError):
        raise BoxError("File is not msgpack as expected")
    except BoxError:
        return BoxList.from_msgpack(filename=file, **kwargs)


def _to_toon(file, encoding, errors, **kwargs):
    if not toon_available:
        raise BoxError(f'File "{file}" is toon but no package is available to open it. Please install "toon_format"')
    try:
        return Box.from_toon(filename=file, encoding=encoding, errors=errors, **kwargs)
    except (ToonDecodeError, ValueError):
        raise BoxError("File is not TOON as expected")
    except BoxError:
        return BoxList.from_toon(filename=file, encoding=encoding, errors=errors, **kwargs)


converters = {
    "json": _to_json,
    "jsn": _to_json,
    "yaml": _to_yaml,
    "yml": _to_yaml,
    "toml": _to_toml,
    "tml": _to_toml,
    "toon": _to_toon,
    "msgpack": _to_msgpack,
    "pack": _to_msgpack,
    "csv": _to_csv,
}  # type: dict[str, Callable]


def box_from_file(
    file: str | PathLike,
    file_type: str | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    **kwargs,
) -> Box | BoxList:
    """
    Loads the provided file and tries to parse it into a Box or BoxList object as appropriate.

    :param file: Location of file
    :param encoding: File encoding
    :param errors: How to handle encoding errors
    :param file_type: manually specify file type: json, toml or yaml
    :return: Box or BoxList
    """

    if not isinstance(file, Path):
        file = Path(file)
    if not file.exists():
        raise BoxError(f'file "{file}" does not exist')
    file_type = file_type or file.suffix
    file_type = file_type.lower().lstrip(".")
    if file_type.lower() in converters:
        return converters[file_type.lower()](file, encoding, errors, **kwargs)  # type: ignore
    raise BoxError(f'"{file_type}" is an unknown type. Please use either csv, toon, toml, msgpack, yaml or json')


def box_from_string(content: str, string_type: str = "json") -> Box | BoxList:
    """
    Parse the provided string into a Box or BoxList object as appropriate.

    :param content: String to parse
    :param string_type: manually specify file type: json, toml or yaml
    :return: Box or BoxList
    """

    if string_type == "json":
        try:
            return Box.from_json(json_string=content)
        except JSONDecodeError:
            raise BoxError("File is not JSON as expected")
        except BoxError:
            return BoxList.from_json(json_string=content)
    elif string_type == "toml":
        try:
            return Box.from_toml(toml_string=content)
        except toml_decode_error:  # type: ignore
            raise BoxError("File is not TOML as expected")
        except BoxError:
            return BoxList.from_toml(toml_string=content)
    elif string_type == "yaml":
        try:
            return Box.from_yaml(yaml_string=content)
        except YAMLError:
            raise BoxError("File is not YAML as expected")
        except BoxError:
            return BoxList.from_yaml(yaml_string=content)
    elif string_type == "toon":
        if not toon_available:
            raise BoxError('toon is unavailable on this system, please install the "toon_format" package')
        try:
            return Box.from_toon(toon_string=content)
        except (ToonDecodeError, ValueError):
            raise BoxError("String is not TOON as expected")
        except BoxError:
            return BoxList.from_toon(toon_string=content)
    else:
        raise BoxError(f"Unsupported string_string of {string_type}")
