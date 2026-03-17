#
# Copyright (C) 2023, 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# Ref. python -c "import tomlkit; help(tomlkit); ..."
#
r"""A backend module to load and dump TOML files.

- Format to support: TOML, https://github.com/toml-lang/toml
- Requirements: tomlkit, https://tomlkit.readthedocs.io
- Development Status :: 4 - Beta
- Limitations: None obvious
- Special options:

  - tomlkit.dump[s]: sort_keys [false] to sort keys.

Changelog:

    .. versionadded:: 0.13.1
"""
from __future__ import annotations

import typing

import tomlkit

from .. import base


class Parser(base.StringStreamFnParser):
    """TOML parser."""

    _cid: typing.ClassVar[str] = "toml.tomlkit"
    _type: typing.ClassVar[str] = "toml"
    _extensions: tuple[str, ...] = ("toml", )
    _ordered: typing.ClassVar[bool] = True
    _dump_opts: tuple[str, ...] = ("sort_keys", )

    _load_from_string_fn = base.to_method(tomlkit.loads)
    _load_from_stream_fn = base.to_method(tomlkit.load)
    _dump_to_string_fn = base.to_method(tomlkit.dumps)
    _dump_to_stream_fn = base.to_method(tomlkit.dump)
