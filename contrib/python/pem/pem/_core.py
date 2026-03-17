# SPDX-FileCopyrightText: 2013 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

"""
Framework agnostic PEM file parsing functions.
"""

from __future__ import annotations

import re

from pathlib import Path

from . import _object_types
from ._object_types import AbstractPEMObject


_PEM_TO_CLASS: dict[bytes, type[AbstractPEMObject]] = {}
for obj in vars(_object_types).values():
    if (
        isinstance(obj, type)
        and issubclass(obj, AbstractPEMObject)
        and obj not in (AbstractPEMObject, _object_types.Key)
    ):
        for pattern in obj._pattern:
            _PEM_TO_CLASS[pattern] = obj

# See https://tools.ietf.org/html/rfc1421
# and https://datatracker.ietf.org/doc/html/rfc4716 for space instead of fifth dash.
_PEM_RE = re.compile(
    b"----[- ]BEGIN ("
    + b"|".join(_PEM_TO_CLASS)
    + b""")[- ]----\r?
(?P<payload>.+?)\r?
----[- ]END \\1[- ]----\r?\n?""",
    re.DOTALL,
)


def parse(pem_str: bytes | str) -> list[AbstractPEMObject]:
    """
    Extract PEM-like objects from *pem_str*.

    Returns:
        list[AbstractPEMObject]: list of :ref:`pem-objects`

    .. versionchanged:: 23.1.0
       *pem_str* can now also be a... :class:`str`.
    """
    return [
        _PEM_TO_CLASS[match.group(1)](match.group(0))
        for match in _PEM_RE.finditer(
            pem_str if isinstance(pem_str, bytes) else pem_str.encode()
        )
    ]


def parse_file(file_name: str | Path) -> list[AbstractPEMObject]:
    """
    Read *file_name* and parse PEM objects from it using :func:`parse`.

    Returns:
        list[AbstractPEMObject]: list of :ref:`pem-objects`

    .. versionchanged:: 23.1.0
       *file_name* can now also be a :class:`~pathlib.Path`.
    """
    return parse(Path(file_name).read_bytes())
