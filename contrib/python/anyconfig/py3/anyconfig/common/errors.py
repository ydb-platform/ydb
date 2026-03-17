#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=too-few-public-methods
"""Basic data types for anyconfig."""
from __future__ import annotations

import typing


class BaseError(RuntimeError):
    """Base Error exception."""

    _msg_fmt: str = "forced_type: {!s}"

    def __init__(self, arg: typing.Any | None = None) -> None:
        """Initialize the format."""
        super().__init__(self._msg_fmt.format(str(arg)))


class UnknownParserTypeError(BaseError):
    """Raise if no parsers were found for given type."""

    _msg_fmt: str = "No parser found for type: {!s}"


class UnknownProcessorTypeError(UnknownParserTypeError):
    """Raise if no processors were found for given type."""


class UnknownFileTypeError(BaseError):
    """Raise if not parsers were found for given file path."""

    _msg_fmt: str = "No parser found for file: {!s}"


class ValidationError(BaseError):
    """Raise if validation failed."""

    _msg_fmt: str = "Validation failed: {!s}"
