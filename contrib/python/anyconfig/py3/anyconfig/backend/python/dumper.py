#
# Copyright (C) 2024 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
r"""A backend module to dump python code conntains data.

- Format to support: Python code
- Requirements: None (built-in)
- Development Status :: 3 - Alpha
- Limitations:

  - This implementaton is very simple and it should be difficult to dump
    complex data using this.

- Special options: None

Changelog:

.. versionadded:: 0.14.0

   - Added builtin data dumper from python code
"""
from __future__ import annotations

import typing

from ..base import (
    InDataExT, ToStringDumperMixin,
)


class Dumper(ToStringDumperMixin):
    """Dumper for objects as python code."""

    def dump_to_string(
        self, cnf: InDataExT, **_kwargs: typing.Any,
    ) -> str:
        """Dump config 'cnf' to a string.

        :param cnf: Configuration data to dump
        :param kwargs: optional keyword parameters to be sanitized :: dict

        :return: string represents the configuration
        """
        return repr(cnf)
