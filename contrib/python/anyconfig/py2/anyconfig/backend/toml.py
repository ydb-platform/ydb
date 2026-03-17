#
# Copyright (C) 2015 - 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# Ref. python -c "import toml; help(toml); ..."
#
# pylint: disable=unused-argument
r"""TOML backend:

- Format to support: TOML, https://github.com/toml-lang/toml
- Requirements: (python) toml module, https://github.com/uiri/toml
- Development Status :: 4 - Beta
- Limitations: None obvious
- Special options:

  - toml.load{s,} only accept '_dict' keyword option but it's used already to
    pass callable to make a container object.

Changelog:

    .. versionadded:: 0.1.0
"""
from __future__ import absolute_import

import toml
import anyconfig.backend.base
from anyconfig.backend.base import to_method


class Parser(anyconfig.backend.base.StringStreamFnParser):
    """
    TOML parser.
    """
    _cid = "toml"
    _type = "toml"
    _extensions = ["toml"]
    _ordered = True
    _load_opts = _dump_opts = _dict_opts = ["_dict"]

    _load_from_string_fn = to_method(toml.loads)
    _load_from_stream_fn = to_method(toml.load)
    _dump_to_string_fn = to_method(toml.dumps)
    _dump_to_stream_fn = to_method(toml.dump)

# vim:sw=4:ts=4:et:
