#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2017-2026 - Chris Griffith - MIT License
from __future__ import annotations

import copy
import re
from collections.abc import Iterable
from os import PathLike
from typing import Any

import box
from box.converters import (
    BOX_PARAMETERS,
    _from_csv,
    _from_json,
    _from_msgpack,
    _from_toml,
    _from_toon,
    _from_yaml,
    _to_csv,
    _to_json,
    _to_msgpack,
    _to_toml,
    _to_toon,
    _to_yaml,
    msgpack_available,
    toon_available,
    toml_read_library,
    yaml_available,
)
from box.exceptions import BoxError, BoxTypeError

_list_pos_re = re.compile(r"\[(\d+)\]")


class BoxList(list):
    """
    Drop in replacement of list, that converts added objects to Box or BoxList
    objects as necessary.
    """

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        # This is required for pickling to work correctly
        obj.box_options = {"box_class": box.Box}
        obj.box_options.update(kwargs)
        obj.box_org_ref = None
        return obj

    def __init__(self, iterable: Iterable | None = None, box_class: type[box.Box] = box.Box, **box_options):
        self.box_options = box_options
        self.box_options["box_class"] = box_class
        self.box_org_ref = iterable
        if iterable:
            for x in iterable:
                self.append(x)
        self.box_org_ref = None
        if box_options.get("frozen_box"):

            def frozen(*args, **kwargs):
                raise BoxError("BoxList is frozen")

            for method in ["append", "extend", "insert", "pop", "remove", "reverse", "sort"]:
                self.__setattr__(method, frozen)

    def __getitem__(self, item):
        if self.box_options.get("box_dots") and isinstance(item, str) and item.startswith("["):
            list_pos = _list_pos_re.search(item)
            value = super().__getitem__(int(list_pos.groups()[0]))
            if len(list_pos.group()) == len(item):
                return value
            return value.__getitem__(item[len(list_pos.group()) :].lstrip("."))
        if isinstance(item, tuple):
            result = self
            for idx in item:
                if isinstance(result, list):
                    result = result[idx]
                else:
                    raise BoxTypeError(f"Cannot numpy-style indexing on {type(result).__name__}.")
            return result
        return super().__getitem__(item)

    def __delitem__(self, key):
        if self.box_options.get("frozen_box"):
            raise BoxError("BoxList is frozen")
        if self.box_options.get("box_dots") and isinstance(key, str) and key.startswith("["):
            list_pos = _list_pos_re.search(key)
            pos = int(list_pos.groups()[0])
            if len(list_pos.group()) == len(key):
                return super().__delitem__(pos)
            if hasattr(self[pos], "__delitem__"):
                return self[pos].__delitem__(key[len(list_pos.group()) :].lstrip("."))  # type: ignore
        super().__delitem__(key)

    def __setitem__(self, key, value):
        if self.box_options.get("frozen_box"):
            raise BoxError("BoxList is frozen")
        if self.box_options.get("box_dots") and isinstance(key, str) and key.startswith("["):
            list_pos = _list_pos_re.search(key)
            pos = int(list_pos.groups()[0])
            if pos >= len(self) and self.box_options.get("default_box"):
                self.extend([None] * (pos - len(self) + 1))
            if len(list_pos.group()) == len(key):
                return super().__setitem__(pos, value)
            children = key[len(list_pos.group()) :].lstrip(".")
            if self.box_options.get("default_box"):
                if children[0] == "[":
                    super().__setitem__(pos, box.BoxList(**self.box_options))
                else:
                    super().__setitem__(pos, self.box_options.get("box_class")(**self.box_options))
            return super().__getitem__(pos).__setitem__(children, value)
        super().__setitem__(key, value)

    def _is_intact_type(self, obj):
        if self.box_options.get("box_intact_types") and isinstance(obj, self.box_options["box_intact_types"]):
            return True
        return False

    def _convert(self, p_object):
        if isinstance(p_object, dict) and not self._is_intact_type(p_object):
            p_object = self.box_options["box_class"](p_object, **self.box_options)
        elif isinstance(p_object, box.Box):
            p_object._box_config.update(self.box_options)
        if isinstance(p_object, list) and not self._is_intact_type(p_object):
            p_object = (
                self
                if p_object is self or p_object is self.box_org_ref
                else self.__class__(p_object, **self.box_options)
            )
        elif isinstance(p_object, BoxList):
            p_object.box_options.update(self.box_options)
        return p_object

    def append(self, p_object):
        super().append(self._convert(p_object))

    def extend(self, iterable):
        for item in iterable:
            self.append(item)

    def insert(self, index, p_object):
        super().insert(index, self._convert(p_object))

    def _dotted_helper(self) -> list[str]:
        keys = []
        for idx, item in enumerate(self):
            added = False
            if isinstance(item, box.Box):
                for key in item.keys(dotted=True):
                    keys.append(f"[{idx}].{key}")
                    added = True
            elif isinstance(item, BoxList):
                for key in item._dotted_helper():
                    keys.append(f"[{idx}]{key}")
                    added = True
            if not added:
                keys.append(f"[{idx}]")
        return keys

    def __repr__(self):
        return f"{self.__class__.__name__}({self.to_list()})"

    def __str__(self):
        return str(self.to_list())

    def __copy__(self):
        return self.__class__((x for x in self), **self.box_options)

    def __deepcopy__(self, memo=None):
        out = self.__class__()
        memo = memo or {}
        memo[id(self)] = out
        for k in self:
            out.append(copy.deepcopy(k, memo=memo))
        return out

    def __hash__(self) -> int:  # type: ignore[override]
        if self.box_options.get("frozen_box"):
            hashing = 98765
            hashing ^= hash(tuple(self))
            return hashing
        raise BoxTypeError("unhashable type: 'BoxList'")

    def to_list(self) -> list:
        new_list: list[Any] = []
        for x in self:
            if x is self:
                new_list.append(new_list)
            elif isinstance(x, box.Box):
                new_list.append(x.to_dict())
            elif isinstance(x, BoxList):
                new_list.append(x.to_list())
            else:
                new_list.append(x)
        return new_list

    def to_json(
        self,
        filename: str | PathLike | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
        multiline: bool = False,
        **json_kwargs,
    ):
        """
        Transform the BoxList object into a JSON string.

        :param filename: If provided will save to file
        :param encoding: File encoding
        :param errors: How to handle encoding errors
        :param multiline: Put each item in list onto it's own line
        :param json_kwargs: additional arguments to pass to json.dump(s)
        :return: string of JSON or return of `json.dump`
        """
        if filename and multiline:
            lines = [_to_json(item, filename=None, encoding=encoding, errors=errors, **json_kwargs) for item in self]
            with open(filename, "w", encoding=encoding, errors=errors) as f:
                f.write("\n".join(lines))
        else:
            return _to_json(self.to_list(), filename=filename, encoding=encoding, errors=errors, **json_kwargs)

    @classmethod
    def from_json(
        cls,
        json_string: str | None = None,
        filename: str | PathLike | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
        multiline: bool = False,
        **kwargs,
    ):
        """
        Transform a json object string into a BoxList object. If the incoming
        json is a dict, you must use Box.from_json.

        :param json_string: string to pass to `json.loads`
        :param filename: filename to open and pass to `json.load`
        :param encoding: File encoding
        :param errors: How to handle encoding errors
        :param multiline: One object per line
        :param kwargs: parameters to pass to `Box()` or `json.loads`
        :return: BoxList object from json data
        """
        box_args = {}
        for arg in list(kwargs.keys()):
            if arg in BOX_PARAMETERS:
                box_args[arg] = kwargs.pop(arg)

        data = _from_json(
            json_string, filename=filename, encoding=encoding, errors=errors, multiline=multiline, **kwargs
        )

        if not isinstance(data, list):
            raise BoxError(f"json data not returned as a list, but rather a {type(data).__name__}")
        return cls(data, **box_args)

    if yaml_available:

        def to_yaml(
            self,
            filename: str | PathLike | None = None,
            default_flow_style: bool = False,
            encoding: str = "utf-8",
            errors: str = "strict",
            width: int = 120,
            **yaml_kwargs,
        ):
            """
            Transform the BoxList object into a YAML string.

            :param filename:  If provided will save to file
            :param default_flow_style: False will recursively dump dicts
            :param encoding: File encoding
            :param errors: How to handle encoding errors
            :param width: Line width for YAML output
            :param yaml_kwargs: additional arguments to pass to yaml.dump
            :return: string of YAML or return of `yaml.dump`
            """
            return _to_yaml(
                self.to_list(),
                filename=filename,
                default_flow_style=default_flow_style,
                encoding=encoding,
                errors=errors,
                width=width,
                **yaml_kwargs,
            )

        @classmethod
        def from_yaml(
            cls,
            yaml_string: str | None = None,
            filename: str | PathLike | None = None,
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            """
            Transform a yaml object string into a BoxList object.

            :param yaml_string: string to pass to `yaml.load`
            :param filename: filename to open and pass to `yaml.load`
            :param encoding: File encoding
            :param errors: How to handle encoding errors
            :param kwargs: parameters to pass to `BoxList()` or `yaml.load`
            :return: BoxList object from yaml data
            """
            box_args = {}
            for arg in list(kwargs.keys()):
                if arg in BOX_PARAMETERS:
                    box_args[arg] = kwargs.pop(arg)

            data = _from_yaml(yaml_string=yaml_string, filename=filename, encoding=encoding, errors=errors, **kwargs)
            if not data:
                return cls(**box_args)
            if not isinstance(data, list):
                raise BoxError(f"yaml data not returned as a list but rather a {type(data).__name__}")
            return cls(data, **box_args)

    else:

        def to_yaml(
            self,
            filename: str | PathLike | None = None,
            default_flow_style: bool = False,
            encoding: str = "utf-8",
            errors: str = "strict",
            width: int = 120,
            **yaml_kwargs,
        ):
            raise BoxError('yaml is unavailable on this system, please install the "ruamel.yaml" or "PyYAML" package')

        @classmethod
        def from_yaml(
            cls,
            yaml_string: str | None = None,
            filename: str | PathLike | None = None,
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            raise BoxError('yaml is unavailable on this system, please install the "ruamel.yaml" or "PyYAML" package')

    if toml_read_library is not None:

        def to_toml(
            self,
            filename: str | PathLike | None = None,
            key_name: str = "toml",
            encoding: str = "utf-8",
            errors: str = "strict",
        ):
            """
            Transform the BoxList object into a toml string.

            :param filename: File to write toml object too
            :param key_name: Specify the name of the key to store the string under
                (cannot directly convert to toml)
            :param encoding: File encoding
            :param errors: How to handle encoding errors
            :return: string of TOML (if no filename provided)
            """
            return _to_toml({key_name: self.to_list()}, filename=filename, encoding=encoding, errors=errors)

    else:

        def to_toml(
            self,
            filename: str | PathLike | None = None,
            key_name: str = "toml",
            encoding: str = "utf-8",
            errors: str = "strict",
        ):
            raise BoxError('toml is unavailable on this system, please install the "tomli-w" package')

    if toml_read_library is not None:

        @classmethod
        def from_toml(
            cls,
            toml_string: str | None = None,
            filename: str | PathLike | None = None,
            key_name: str = "toml",
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            """
            Transforms a toml string or file into a BoxList object

            :param toml_string: string to pass to `toml.load`
            :param filename: filename to open and pass to `toml.load`
            :param key_name: Specify the name of the key to pull the list from
                (cannot directly convert from toml)
            :param encoding: File encoding
            :param errors: How to handle encoding errors
            :param kwargs: parameters to pass to `Box()`
            :return:
            """
            box_args = {}
            for arg in list(kwargs.keys()):
                if arg in BOX_PARAMETERS:
                    box_args[arg] = kwargs.pop(arg)

            data = _from_toml(toml_string=toml_string, filename=filename, encoding=encoding, errors=errors)
            if key_name not in data:
                raise BoxError(f"{key_name} was not found.")
            return cls(data[key_name], **box_args)

    else:

        @classmethod
        def from_toml(
            cls,
            toml_string: str | None = None,
            filename: str | PathLike | None = None,
            key_name: str = "toml",
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            raise BoxError('toml is unavailable on this system, please install the "toml" package')

    if msgpack_available:

        def to_msgpack(self, filename: str | PathLike | None = None, **kwargs):
            """
            Transform the BoxList object into a toml string.

            :param filename: File to write toml object too
            :return: string of TOML (if no filename provided)
            """
            return _to_msgpack(self.to_list(), filename=filename, **kwargs)

        @classmethod
        def from_msgpack(cls, msgpack_bytes: bytes | None = None, filename: str | PathLike | None = None, **kwargs):
            """
            Transforms a toml string or file into a BoxList object

            :param msgpack_bytes: string to pass to `msgpack.packb`
            :param filename: filename to open and pass to `msgpack.pack`
            :param kwargs: parameters to pass to `Box()`
            :return:
            """
            box_args = {}
            for arg in list(kwargs.keys()):
                if arg in BOX_PARAMETERS:
                    box_args[arg] = kwargs.pop(arg)

            data = _from_msgpack(msgpack_bytes=msgpack_bytes, filename=filename, **kwargs)
            if not isinstance(data, list):
                raise BoxError(f"msgpack data not returned as a list but rather a {type(data).__name__}")
            return cls(data, **box_args)

    else:

        def to_msgpack(self, filename: str | PathLike | None = None, **kwargs):
            raise BoxError('msgpack is unavailable on this system, please install the "msgpack" package')

        @classmethod
        def from_msgpack(
            cls,
            msgpack_bytes: bytes | None = None,
            filename: str | PathLike | None = None,
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            raise BoxError('msgpack is unavailable on this system, please install the "msgpack" package')

    if toon_available:

        def to_toon(
            self, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict", **kwargs
        ):
            """
            Transform the BoxList object into a TOON string.

            :param filename: File to write TOON object too
            :param encoding: File encoding
            :param errors: How to handle encoding errors
            :param kwargs: parameters to pass to `toon_format.encode`
            :return: string of TOON (if no filename provided)
            """
            return _to_toon(self.to_list(), filename=filename, encoding=encoding, errors=errors, **kwargs)

        @classmethod
        def from_toon(
            cls,
            toon_string: str | None = None,
            filename: str | PathLike | None = None,
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            """
            Transforms a TOON string or file into a BoxList object

            :param toon_string: string to pass to `toon_format.decode`
            :param filename: filename to open and pass to `toon_format.decode`
            :param encoding: File encoding
            :param errors: How to handle encoding errors
            :param kwargs: parameters to pass to `BoxList()`
            :return: BoxList object
            """
            box_args = {}
            for arg in list(kwargs.keys()):
                if arg in BOX_PARAMETERS:
                    box_args[arg] = kwargs.pop(arg)

            data = _from_toon(toon_string=toon_string, filename=filename, encoding=encoding, errors=errors, **kwargs)
            if not isinstance(data, list):
                raise BoxError(f"toon data not returned as a list but rather a {type(data).__name__}")
            return cls(data, **box_args)

    else:

        def to_toon(
            self, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict", **kwargs
        ):
            raise BoxError('toon is unavailable on this system, please install the "toon_format" package')

        @classmethod
        def from_toon(
            cls,
            toon_string: str | None = None,
            filename: str | PathLike | None = None,
            encoding: str = "utf-8",
            errors: str = "strict",
            **kwargs,
        ):
            raise BoxError('toon is unavailable on this system, please install the "toon_format" package')

    def to_csv(self, filename: str | PathLike | None = None, encoding: str = "utf-8", errors: str = "strict"):
        return _to_csv(self, filename=filename, encoding=encoding, errors=errors)

    @classmethod
    def from_csv(
        cls,
        csv_string: str | None = None,
        filename: str | PathLike | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ):
        return cls(_from_csv(csv_string=csv_string, filename=filename, encoding=encoding, errors=errors))
