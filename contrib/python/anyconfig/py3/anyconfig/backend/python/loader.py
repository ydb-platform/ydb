#
# Copyright (C) 2023 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
r"""A backend module to load python code conntains data.

- Format to support: Python code
- Requirements: None (built-in)
- Development Status :: 3 - Alpha
- Limitations:

  - This module will load data as it is. In other words, some options like
    ac_dict and ac_ordered do not affetct at all.
  - Some primitive data expressions support only
  - It might have some vulnerabilities for DoS and aribitary code execution
    (ACE) attacks

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

import pathlib
import tempfile
import typing

from ... import ioinfo
from ..base import (
    IoiT, InDataExT, LoaderMixin,
)

from . import utils


def load_from_temp_file(
    content: str, **opts: typing.Any,
) -> InDataExT:
    """Dump `content` to tempoary file and load from it.

    :param content: A str to load data from
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        path = pathlib.Path(tmpdir) / "mod.py"
        path.write_text(content, encoding="utf-8")

        return utils.load_from_path(
            path, allow_exec=opts.get("allow_exec", False),
        )


class Loader(LoaderMixin):
    """Loader for python code files."""

    _allow_primitives: typing.ClassVar[bool] = True
    _load_opts: tuple[str, ...] = ("allow_exec", )

    def loads(
        self, content: str, **options: typing.Any,
    ) -> InDataExT:
        """Load config from given string 'content' after some checks.

        :param content: Config file content
        :param options:
            It will be ignored at all except for 'allow_exec' opion to allow
            execution of the code

        :return:
            dict or dict-like object holding input data or primitives
        """
        allow_exec = options.get("allow_exec", False)

        if allow_exec and content and utils.DATA_VAR_NAME in content:
            return load_from_temp_file(content, allow_exec=allow_exec)

        return utils.load_literal_data_from_string(content)

    def load(
        self, ioi: IoiT, **options: typing.Any,
    ) -> InDataExT:
        """Load config from ``ioi``.

        :param ioi:
            'anyconfig.ioinfo.IOInfo' namedtuple object provides various info
            of input object to load data from

        :param options:
            options will be passed to backend specific loading functions.
            please note that options have to be sanitized w/
            :func:`anyconfig.utils.filter_options` later to filter out options
            not in _load_opts.

        :return: dict or dict-like object holding configurations
        """
        allow_exec = options.get("allow_exec", False)

        if not ioi:
            return {}

        if ioinfo.is_stream(ioi):
            return load_from_temp_file(
                typing.cast("typing.IO", ioi.src).read(),
                allow_exec=allow_exec,
            )

        return utils.load_from_path(
            pathlib.Path(ioi.path), allow_exec=allow_exec,
        )
