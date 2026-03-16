#
# Copyright (C) 2015 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=unused-argument
"""Default (dummy) implementation."""
from __future__ import annotations

import typing

from .jsonschema import generator

if typing.TYPE_CHECKING:
    from .datatypes import (
        InDataT, InDataExT, ResultT,
    )


def validate(
    data: InDataExT, schema: InDataExT, *,
    ac_schema_safe: bool = True, ac_schema_errors: bool = False,
    **options: typing.Any,
) -> ResultT:
    """Provide a dummy function does not validate at all in actual."""
    return (True, "Validation module (jsonschema) is not available")


def is_valid(
    data: InDataExT, schema: InDataExT, *,
    ac_schema_safe: bool = True, ac_schema_errors: bool = False,
    **options: typing.Any,
) -> bool:
    """Provide a dummy function never raise exceptions."""
    return True


def gen_schema(data: InDataExT, **options: typing.Any) -> InDataT:
    """Provide a dummy function generates an empty dict in actual."""
    return generator.gen_schema(data, **options)
