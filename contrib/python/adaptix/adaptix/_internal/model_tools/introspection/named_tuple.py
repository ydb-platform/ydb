from types import MappingProxyType
from typing import Any

from ...type_tools import get_all_type_hints, is_named_tuple_class
from ..definitions import (
    DefaultValue,
    FullShape,
    InputField,
    InputShape,
    IntrospectionError,
    NoDefault,
    OutputField,
    OutputShape,
    Param,
    ParamKind,
    Shape,
    create_key_accessor,
)


def get_named_tuple_shape(tp) -> FullShape:
    if not is_named_tuple_class(tp):
        raise IntrospectionError

    type_hints = get_all_type_hints(tp)
    if tuple in tp.__bases__:
        overriden_types = frozenset(tp._fields)
    else:
        overriden_types = frozenset(tp.__annotations__.keys() & set(tp._fields))

    # noinspection PyProtectedMember
    input_shape = InputShape(
        constructor=tp,
        kwargs=None,
        fields=tuple(
            InputField(
                id=field_id,
                type=type_hints.get(field_id, Any),
                default=(
                    DefaultValue(tp._field_defaults[field_id])
                    if field_id in tp._field_defaults else
                    NoDefault()
                ),
                is_required=field_id not in tp._field_defaults,
                metadata=MappingProxyType({}),
                original=None,
            )
            for field_id in tp._fields
        ),
        params=tuple(
            Param(
                field_id=field_id,
                name=field_id,
                kind=ParamKind.POS_OR_KW,
            )
            for field_id in tp._fields
        ),
        overriden_types=overriden_types,
    )

    return Shape(
        input=input_shape,
        output=OutputShape(
            fields=tuple(
                OutputField(
                    id=fld.id,
                    type=fld.type,
                    default=fld.default,
                    metadata=fld.metadata,
                    accessor=create_key_accessor(
                        key=idx,
                        access_error=None,
                    ),
                    original=None,
                )
                for idx, fld in enumerate(input_shape.fields)
            ),
            overriden_types=overriden_types,
        ),
    )
