#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

# Abstract converter functions for use in any Box class

import csv
import json
from collections.abc import Callable
from io import StringIO
from os import PathLike
from pathlib import Path
from typing import Any

from box.exceptions import BoxError

pyyaml_available = True
ruamel_available = True
msgpack_available = True

try:
    from ruamel.yaml import version_info, YAML
except ImportError:
    ruamel_available = False
else:
    if version_info[1] < 17:
        ruamel_available = False

try:
    import yaml
except ImportError:
    pyyaml_available = False

MISSING_PARSER_ERROR = "No YAML Parser available, please install ruamel.yaml>=0.17 or PyYAML"

toml_read_library: Any | None = None
toml_write_library: Any | None = None
toml_decode_error: Callable | None = None

__all__ = [
    "_to_json",
    "_to_yaml",
    "_to_toml",
    "_to_csv",
    "_to_msgpack",
    "_to_toon",
    "_from_json",
    "_from_yaml",
    "_from_toml",
    "_from_csv",
    "_from_msgpack",
    "_from_toon",
]


class BoxTomlDecodeError(BoxError):
    """Toml Decode Error"""


try:
    import toml
except ImportError:
    pass
else:
    toml_read_library = toml
    toml_write_library = toml
    toml_decode_error = toml.TomlDecodeError

    class BoxTomlDecodeError(BoxError, toml.TomlDecodeError):  # type: ignore
        """Toml Decode Error"""


try:
    import tomllib
except ImportError:
    pass
else:
    toml_read_library = tomllib
    toml_decode_error = tomllib.TOMLDecodeError

    class BoxTomlDecodeError(BoxError, tomllib.TOMLDecodeError):  # type: ignore
        """Toml Decode Error"""


try:
    import tomli
except ImportError:
    pass
else:
    toml_read_library = tomli
    toml_decode_error = tomli.TOMLDecodeError

    class BoxTomlDecodeError(BoxError, tomli.TOMLDecodeError):  # type: ignore
        """Toml Decode Error"""


try:
    import tomli_w
except ImportError:
    pass
else:
    toml_write_library = tomli_w


try:
    import msgpack  # type: ignore
except ImportError:
    msgpack = None  # type: ignore
    msgpack_available = False

toon_available = True

try:
    from toon_format import encode as toon_encode, decode as toon_decode
except ImportError:
    toon_available = False

yaml_available = pyyaml_available or ruamel_available

BOX_PARAMETERS = (
    "default_box",
    "default_box_attr",
    "default_box_none_transform",
    "default_box_create_on_get",
    "frozen_box",
    "camel_killer_box",
    "conversion_box",
    "modify_tuples_box",
    "box_safe_prefix",
    "box_duplicates",
    "box_intact_types",
    "box_dots",
    "box_dots_exclude",
    "box_recast",
    "box_class",
    "box_namespace",
)


def _exists(filename: str | PathLike, create: bool = False) -> Path:
    path = Path(filename)
    if create:
        try:
            path.touch(exist_ok=True)
        except OSError as err:
            raise BoxError(f"Could not create file {filename} - {err}")
        else:
            return path
    if not path.exists():
        raise BoxError(f'File "{filename}" does not exist')
    if not path.is_file():
        raise BoxError(f"{filename} is not a file")
    return path


def _to_json(
    obj, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict", **json_kwargs
):
    if filename:
        _exists(filename, create=True)
        with open(filename, "w", encoding=encoding, errors=errors) as f:
            json.dump(obj, f, ensure_ascii=False, **json_kwargs)
    else:
        return json.dumps(obj, ensure_ascii=False, **json_kwargs)


def _from_json(
    json_string: str | None = None,
    filename: str | PathLike | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    multiline: bool = False,
    **kwargs,
):
    if filename:
        with open(filename, "r", encoding=encoding, errors=errors) as f:
            if multiline:
                data = [
                    json.loads(line.strip(), **kwargs)
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]
            else:
                data = json.load(f, **kwargs)
    elif json_string:
        data = json.loads(json_string, **kwargs)
    else:
        raise BoxError("from_json requires a string or filename")
    return data


def _to_yaml(
    obj,
    filename: str | PathLike | None = None,
    default_flow_style: bool = False,
    encoding: str = "utf-8",
    errors: str = "strict",
    ruamel_typ: str = "rt",
    ruamel_attrs: dict | None = None,
    width: int = 120,
    **yaml_kwargs,
):
    if not ruamel_attrs:
        ruamel_attrs = {}
    if filename:
        _exists(filename, create=True)
        with open(filename, "w", encoding=encoding, errors=errors) as f:
            if ruamel_available:
                yaml_dumper = YAML(typ=ruamel_typ)
                yaml_dumper.default_flow_style = default_flow_style
                yaml_dumper.width = width
                for attr, value in ruamel_attrs.items():
                    setattr(yaml_dumper, attr, value)
                return yaml_dumper.dump(obj, stream=f, **yaml_kwargs)
            elif pyyaml_available:
                return yaml.dump(obj, stream=f, default_flow_style=default_flow_style, width=width, **yaml_kwargs)
            else:
                raise BoxError(MISSING_PARSER_ERROR)

    else:
        if ruamel_available:
            yaml_dumper = YAML(typ=ruamel_typ)
            yaml_dumper.default_flow_style = default_flow_style
            yaml_dumper.width = width
            for attr, value in ruamel_attrs.items():
                setattr(yaml_dumper, attr, value)
            with StringIO() as string_stream:
                yaml_dumper.dump(obj, stream=string_stream, **yaml_kwargs)
                return string_stream.getvalue()
        elif pyyaml_available:
            return yaml.dump(obj, default_flow_style=default_flow_style, width=width, **yaml_kwargs)
        else:
            raise BoxError(MISSING_PARSER_ERROR)


def _from_yaml(
    yaml_string: str | None = None,
    filename: str | PathLike | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    ruamel_typ: str = "rt",
    ruamel_attrs: dict | None = None,
    **kwargs,
):
    if not ruamel_attrs:
        ruamel_attrs = {}
    if filename:
        _exists(filename)
        with open(filename, "r", encoding=encoding, errors=errors) as f:
            if ruamel_available:
                yaml_loader = YAML(typ=ruamel_typ)
                for attr, value in ruamel_attrs.items():
                    setattr(yaml_loader, attr, value)
                data = yaml_loader.load(stream=f)
            elif pyyaml_available:
                if "Loader" not in kwargs:
                    kwargs["Loader"] = yaml.SafeLoader
                data = yaml.load(f, **kwargs)
            else:
                raise BoxError(MISSING_PARSER_ERROR)
    elif yaml_string:
        if ruamel_available:
            yaml_loader = YAML(typ=ruamel_typ)
            for attr, value in ruamel_attrs.items():
                setattr(yaml_loader, attr, value)
            data = yaml_loader.load(stream=yaml_string)
        elif pyyaml_available:
            if "Loader" not in kwargs:
                kwargs["Loader"] = yaml.SafeLoader
            data = yaml.load(yaml_string, **kwargs)
        else:
            raise BoxError(MISSING_PARSER_ERROR)
    else:
        raise BoxError("from_yaml requires a string or filename")
    return data


def _to_toml(obj, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict"):
    if filename:
        _exists(filename, create=True)
        if toml_write_library.__name__ == "toml":  # type: ignore
            with open(filename, "w", encoding=encoding, errors=errors) as f:
                try:
                    toml_write_library.dump(obj, f)  # type: ignore
                except toml_decode_error as err:  # type: ignore
                    raise BoxTomlDecodeError(err) from err
        else:
            with open(filename, "wb") as f:
                try:
                    toml_write_library.dump(obj, f)  # type: ignore
                except toml_decode_error as err:  # type: ignore
                    raise BoxTomlDecodeError(err) from err
    else:
        try:
            return toml_write_library.dumps(obj)  # type: ignore
        except toml_decode_error as err:  # type: ignore
            raise BoxTomlDecodeError(err) from err


def _from_toml(
    toml_string: str | None = None,
    filename: str | PathLike | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
):
    if filename:
        _exists(filename)
        if toml_read_library.__name__ == "toml":  # type: ignore
            with open(filename, "r", encoding=encoding, errors=errors) as f:
                data = toml_read_library.load(f)  # type: ignore
        else:
            with open(filename, "rb") as f:
                data = toml_read_library.load(f)  # type: ignore
    elif toml_string:
        data = toml_read_library.loads(toml_string)  # type: ignore
    else:
        raise BoxError("from_toml requires a string or filename")
    return data


def _to_msgpack(obj, filename: str | PathLike | None = None, **kwargs):
    if filename:
        _exists(filename, create=True)
        with open(filename, "wb") as f:
            msgpack.pack(obj, f, **kwargs)
    else:
        return msgpack.packb(obj, **kwargs)


def _from_msgpack(msgpack_bytes: bytes | None = None, filename: str | PathLike | None = None, **kwargs):
    if filename:
        _exists(filename)
        with open(filename, "rb") as f:
            data = msgpack.unpack(f, **kwargs)
    elif msgpack_bytes:
        data = msgpack.unpackb(msgpack_bytes, **kwargs)
    else:
        raise BoxError("from_msgpack requires a string or filename")
    return data


def _to_toon(obj, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict", **kwargs):
    if filename:
        _exists(filename, create=True)
        with open(filename, "w", encoding=encoding, errors=errors) as f:
            f.write(toon_encode(obj, **kwargs))
    else:
        return toon_encode(obj, **kwargs)


def _from_toon(
    toon_string: str | None = None,
    filename: str | PathLike | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    **kwargs,
):
    if filename:
        _exists(filename)
        with open(filename, "r", encoding=encoding, errors=errors) as f:
            data = toon_decode(f.read(), **kwargs)
    elif toon_string:
        data = toon_decode(toon_string, **kwargs)
    else:
        raise BoxError("from_toon requires a string or filename")
    return data


def _to_csv(
    box_list, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict", **kwargs
):
    csv_column_names = list(box_list[0].keys())
    for row in box_list:
        if list(row.keys()) != csv_column_names:
            raise BoxError("BoxList must contain the same dictionary structure for every item to convert to csv")

    if filename:
        _exists(filename, create=True)
        out_data = open(filename, "w", encoding=encoding, errors=errors, newline="")
    else:
        out_data = StringIO("")
    writer = csv.DictWriter(out_data, fieldnames=csv_column_names, **kwargs)
    writer.writeheader()
    for data in box_list:
        writer.writerow(data)
    if not filename:
        return out_data.getvalue()  # type: ignore
    out_data.close()


def _from_csv(
    csv_string: str | None = None,
    filename: str | PathLike | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    **kwargs,
):
    if csv_string:
        with StringIO(csv_string) as cs:
            reader = csv.DictReader(cs)
            return [row for row in reader]
    _exists(filename)  # type: ignore
    with open(filename, "r", encoding=encoding, errors=errors, newline="") as f:  # type: ignore
        reader = csv.DictReader(f, **kwargs)
        return [row for row in reader]
