#  Copyright (c) Kuba Szczodrzyński 2023-1-7.

from dataclasses import MISSING
from typing import Any

from datastruct.types import FieldType, FormatType, Value

from ._utils import build_field


def field(fmt: FormatType, *, default=..., default_factory=MISSING):
    return build_field(
        ftype=FieldType.FIELD,
        default=default,
        default_factory=default_factory,
        # meta
        fmt=fmt,
    )


def subfield(*, default_factory=MISSING, **kwargs):
    # don't allow 'default' for subfields, as they're always mutable
    return build_field(
        ftype=FieldType.FIELD,
        default_factory=default_factory,
        # meta
        kwargs=kwargs,
    )


def built(fmt: FormatType, builder: Value[Any], *, always: bool = True):
    return build_field(
        ftype=FieldType.FIELD,
        # meta
        fmt=fmt,
        builder=builder,
        always=always,
    )
