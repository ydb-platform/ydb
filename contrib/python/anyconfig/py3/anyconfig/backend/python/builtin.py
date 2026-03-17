#
# Copyright (C) 2023 - 2025 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
r"""A backend module to load and dump python code conntains data.

- Format to support: Python code
- Requirements: None (built-in)
- Development Status :: 3 - Alpha
- Limitations:

  - This module will load data as it is. In other words, some options like
    ac_dict and ac_ordered do not affetct at all.
  - Some primitive data expressions support only
  - It might have some vulnerabilities for DoS and aribitary code execution
    (ACE) attacks
  - It's very simple and should be difficult to dump complex data using this

- Special options:

  - allow_exec: bool [False]: Allow execution of the input python code on load
    input files.  It may cause vulnerabilities for aribitary code execution
    (ACE) attacks. So you should set True only if you sure inputs are safe from
    reliable sources.

Changelog:

.. versionadded:: 0.14.0

   - Added builtin data loader from python code
"""
from __future__ import annotations

import typing

from .. import base
from . import (
    loader, dumper,
)


class Parser(base.Parser, loader.Loader, dumper.Dumper):
    """Parser for python code files."""

    _cid: typing.ClassVar[str] = "python.builtin"
    _type: typing.ClassVar[str] = "python"
    _extensions: tuple[str, ...] = ("py", )
