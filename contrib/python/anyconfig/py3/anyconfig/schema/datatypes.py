#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=unused-import
"""Some common constants, utility functions and so on."""
from __future__ import annotations

import typing

from ..common import (  # noqa: F401
    ValidationError, InDataT, InDataExT,
)


ResultT = tuple[bool, typing.Union[str, list[str]]]
