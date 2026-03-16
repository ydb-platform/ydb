#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Misc global constants, variables, classes and so on."""
from __future__ import annotations

import typing

try:
    from .jinja2 import try_render
    SUPPORTED: bool = True
except ImportError:  # jinja2 may not be available.
    SUPPORTED = False

    def try_render(
        filepath: str | None = None,
        content: str | None = None,
        **_options: typing.Any,
    ) -> str | None:
        """Provide a dummy function does nothing but returns None."""
        if filepath and content:
            return None

        return None


__all__ = [
    "try_render",
]
