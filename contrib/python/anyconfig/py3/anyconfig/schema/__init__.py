#
# Copyright (C) 2021 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Schema generation and validation."""
from __future__ import annotations

from .jsonschema.generator import gen_schema

try:
    from .jsonschema.validator import validate, is_valid

    VALIDATORS = {
        "jsonschema": validate,
    }
    SUPPORTED: bool = True
except ImportError:
    from .default import validate, is_valid  # noqa: F401
    VALIDATORS = {}
    SUPPORTED = False


GENERATORS = {
    "jsonschema": gen_schema,
}

_all__ = [
    "validate", "is_valid", "gen_schema",
    "VALIDATORS", "GENERATORS", "SUPPORTED",
]
