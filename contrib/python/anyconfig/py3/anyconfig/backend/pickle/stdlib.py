#
# Copyright (C) 2017 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
r"""A backend module to load and dump pickle files.

- Format to support: Pickle
- Requirements: It should be available always.

  - pickle/cPickle in python 2 standard library:
    https://docs.python.org/2/library/pickle.html

  - pickle in python 3 standard library:
    https://docs.python.org/3/library/pickle.html

- Development Status :: 4 - Beta
- Limitations: The parser cannot load some primitive data such like '' (empty
  string), ' ' (white space) and [] (empty list) as these are because of the
  implementation of :func:`anyconfig.backend.base.load_with_fn`.
- Special options: All options of pickle.{load{s,},dump{s,}} should work.

Changelog:

.. versionchanged:: 0.9.7

   - Add support of loading primitives other than mapping objects.

.. versionadded:: 0.8.3
"""
from __future__ import annotations

import pickle
import typing

from .. import base


LOAD_OPTS: tuple[str, ...] = ("fix_imports", "encoding", "errors")
DUMP_OPTS: tuple[str, ...] = ("protocol", "fix_imports")


class Parser(base.StringStreamFnParser):
    """Parser for Pickle files."""

    _cid: typing.ClassVar[str] = "pickle.stdlib"
    _type: typing.ClassVar[str] = "pickle"
    _extensions: tuple[str, ...] = ("pkl", "pickle")
    _open_read_mode: typing.ClassVar[str] = "rb"
    _open_write_mode: typing.ClassVar[str] = "wb"

    _load_opts = LOAD_OPTS
    _dump_opts = DUMP_OPTS
    _allow_primitives: typing.ClassVar[bool] = True

    _load_from_string_fn = base.to_method(pickle.loads)
    _load_from_stream_fn = base.to_method(pickle.load)
    _dump_to_string_fn = base.to_method(pickle.dumps)
    _dump_to_stream_fn = base.to_method(pickle.dump)
