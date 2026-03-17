#  Copyright (c) Kuba Szczodrzyński 2023-1-7.

from dataclasses import MISSING, Field
from typing import Any, Dict, Tuple, Type

from datastruct.types import Adapter, AdapterType, Eval, FieldType, Value
from datastruct.utils.fields import field_get_meta

from ._utils import build_field, build_wrapper


def repeat(
    count: Value[int] = None,
    *,
    when: Eval[bool] = None,
    last: Eval[bool] = None,
    length: Eval[int] = None,
    default_factory: Any = MISSING,
):
    if [count, when, last, length].count(None) == 4:
        raise ValueError(
            "At least one of 'count', 'when', 'last' or 'length' has to be set"
        )

    return build_wrapper(
        ftype=FieldType.REPEAT,
        default=...,
        default_factory=default_factory,
        # meta
        count=count,
        when=when,
        last=last,
        length=length,
    )


def cond(condition: Value[bool], *, if_not: Value[Any] = ...):
    return build_wrapper(
        ftype=FieldType.COND,
        # meta
        condition=condition,
        if_not=if_not,
    )


def switch(key: Value[Any]):
    def wrap(fields: Dict[Any, Tuple[Type, Field]] = None, **kwargs):
        fields = fields or {}
        fields.update(kwargs)
        return build_field(
            ftype=FieldType.SWITCH,
            # meta
            key=key,
            fields=fields,
        )

    return wrap


def adapter(
    _adapter: Adapter = None,
    *,
    encode: AdapterType = None,
    decode: AdapterType = None,
):
    if [_adapter, encode and decode].count(None) != 1:
        raise ValueError("Either 'adapter' or 'encode' and 'decode' has to be set")
    if not _adapter:
        _adapter = Adapter()
        _adapter.encode = encode
        _adapter.decode = decode

    def wrap(base: Field):
        meta = field_get_meta(base)
        if meta.ftype != FieldType.FIELD:
            raise TypeError("Can't assign adapters to non-standard fields")
        meta.adapter = _adapter
        return base

    return wrap
