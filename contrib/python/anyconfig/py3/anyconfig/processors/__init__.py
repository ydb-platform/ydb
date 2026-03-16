#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Provide a list of a  :class:`anyconfig.models.processor` and so on.

.. versionchanged:: 0.10.2

   - Split and re-organize the module and add some data types.

.. versionadded:: 0.9.5

   - Add to abstract processors such like Parsers (loaders and dumpers).
"""
from .datatypes import (
    ProcT, ProcClsT, ProcClssT, MaybeProcT,
)
from .processors import Processors
from .utils import (
    list_by_x, load_plugins,
)

__all__ = [
    "ProcT", "ProcClsT", "ProcClssT", "MaybeProcT",
    "Processors",
    "list_by_x", "load_plugins",
]
