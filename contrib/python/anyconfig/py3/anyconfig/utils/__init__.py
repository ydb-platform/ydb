#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Internal utility functions for anyconfig modules.

.. versionchanged:: 0.14.0

   - Add 'is_primitive_type' to test if given object is primiitive type.

.. versionchanged:: 0.10.2

   - Split and re-organize the module and export only some functions.

.. versionadded:: 0.9.5

   - Add to abstract processors such like Parsers (loaders and dumpers).
"""
from .detectors import (
    is_primitive_type, is_iterable, is_dict_like, is_list_like,
)
from .files import get_path_from_stream
from .lists import (
    groupby, concat,
)
from .utils import (
    filter_options, noop,
)


__all__ = [
    "is_primitive_type", "is_iterable", "is_dict_like", "is_list_like",
    "get_path_from_stream",
    "groupby", "concat",
    "filter_options", "noop",
]
