#
# Copyright (C) 2021 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Public API to query data with JMESPath expression."""
try:
    from .query import try_query
    SUPPORTED = True
except ImportError:
    from .default import try_query
    SUPPORTED = False


__all__ = [
    "try_query",
]
