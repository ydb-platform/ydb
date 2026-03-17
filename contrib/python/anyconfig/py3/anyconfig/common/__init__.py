#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Misc global constants, variables, classes and so on."""
from .datatypes import (
    InDataT, InDataExT, PrimitiveT,
)
from .errors import (
    UnknownParserTypeError, UnknownProcessorTypeError, UnknownFileTypeError,
    ValidationError,
)


__all__ = [
    "InDataT", "InDataExT", "PrimitiveT",
    "UnknownParserTypeError", "UnknownProcessorTypeError",
    "UnknownFileTypeError", "ValidationError",
]
