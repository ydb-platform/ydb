#
# Copyright (C) 2011 - 2025 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Common library for YAML backend modules."""
from __future__ import annotations

import typing

from ...utils import filter_options
from .. import base


def filter_from_options(
    key: str, options: dict[str, typing.Any],
) -> dict[str, typing.Any]:
    """Filter a key ``key`` in ``options.

    :param key: Key str in options
    :param options: Mapping object
    :return:
        New mapping object from 'options' in which the item with 'key' filtered

    >>> filter_from_options("a", dict(a=1, b=2))
    {'b': 2}
    """
    return filter_options([k for k in options if k != key], options)


class Parser(base.StreamParser):
    """Parser for YAML files."""

    _type: typing.ClassVar[str] = "yaml"
    _extensions: tuple[str, ...] = ("yaml", "yml")
    _ordered: typing.ClassVar[bool] = True
    _allow_primitives: typing.ClassVar[bool] = True
    _dict_opts: tuple[str, ...] = ("ac_dict", )
