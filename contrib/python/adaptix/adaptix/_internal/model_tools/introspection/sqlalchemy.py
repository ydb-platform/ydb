import inspect
from collections.abc import Mapping
from typing import Any, Generic, Optional, TypeVar

from ...common import TypeHint

try:
    import sqlalchemy
    from sqlalchemy import Column, ColumnCollection
    from sqlalchemy.exc import NoInspectionAvailable
    from sqlalchemy.orm import Mapped, Mapper, RelationshipProperty
    from sqlalchemy.sql.schema import (
        CallableColumnDefault,
        ColumnElement,
        DefaultGenerator,
        ScalarElementColumnDefault,
        Table,
    )
    from sqlalchemy.util import ReadOnlyProperties
except ImportError:
    pass

from ...feature_requirement import HAS_SQLALCHEMY_PKG, HAS_SUPPORTED_SQLALCHEMY_PKG
from ...type_tools import get_all_type_hints, get_generic_args, strip_alias
from ..definitions import (
    ClarifiedIntrospectionError,
    DefaultFactory,
    DefaultValue,
    FullShape,
    InputField,
    InputShape,
    IntrospectionError,
    NoDefault,
    NoTargetPackageError,
    OutputField,
    OutputShape,
    Param,
    ParamKind,
    Shape,
    TooOldPackageError,
    create_attr_accessor,
)

T = TypeVar("T")


class IdWrapper(Generic[T]):
    def __init__(self, value):
        self.value = value

    def __eq__(self, other: object):
        if isinstance(other, IdWrapper):
            return self.value is other.value
        return NotImplemented

    def __repr__(self):
        return f"{type(self).__name__}({self.value})"


def _is_context_sensitive(default: "CallableColumnDefault"):
    try:
        wrapped_callable = default.arg.__wrapped__  # type: ignore[attr-defined]
    except AttributeError:
        return True

    parameters = inspect.signature(wrapped_callable).parameters
    return len(parameters) > 0


def _unwrap_mapped_annotation(type_hint: TypeHint) -> TypeHint:
    if strip_alias(type_hint) == Mapped:
        return get_generic_args(type_hint)[0]
    return type_hint


def _get_type_for_column(column: "ColumnElement", type_hints: Mapping[str, TypeHint]):
    try:
        return _unwrap_mapped_annotation(type_hints[column.name])
    except KeyError:
        if column.nullable:
            return Optional[column.type.python_type]
        return column.type.python_type


def _get_type_for_relationship(relationship: "RelationshipProperty", type_hints: Mapping[str, TypeHint]):
    try:
        return _unwrap_mapped_annotation(type_hints[relationship.key])
    except KeyError:
        if relationship.uselist:
            return list[relationship.entity.class_]  # type: ignore[name-defined]
        return Optional[relationship.entity.class_]


def _get_default(column_default: "Optional[DefaultGenerator]"):
    if isinstance(column_default, CallableColumnDefault):
        if _is_context_sensitive(column_default):
            return NoDefault()
        return DefaultFactory(factory=column_default.arg.__wrapped__)  # type: ignore[attr-defined]
    if isinstance(column_default, ScalarElementColumnDefault):
        return DefaultValue(value=column_default.arg)
    return NoDefault()


def _is_input_required_for_column(column: "ColumnElement", autoincrement_column: "Optional[Column[int]]"):
    return not (
        # columns constrained by FK are not required since they can be specified by instances
        column.default is not None
        or column.nullable
        or column.server_default is not None
        or column.foreign_keys
        or column is autoincrement_column
    )


def _get_autoincrement_column(table: "Table"):
    try:
        return table.autoincrement_column
    except AttributeError:
        # for old sqlalchemy
        return table._autoincrement_column


def _get_input_shape(
    tp: TypeHint,
    table: "Table",
    columns: "ColumnCollection",
    relationships: "ReadOnlyProperties[RelationshipProperty[Any]]",
    type_hints: Mapping[str, TypeHint],
) -> InputShape:
    # FromClause has no autoincrement_column
    autoincrement_column = _get_autoincrement_column(table)
    fields = []
    params = []
    for column in columns:
        if not isinstance(column, sqlalchemy.Column):
            continue

        fields.append(
            InputField(
                id=column.key,
                type=_get_type_for_column(column, type_hints),
                default=_get_default(column.default),
                is_required=_is_input_required_for_column(column, autoincrement_column),
                metadata=column.info,
                original=IdWrapper(column),
            ),
        )
        params.append(
            Param(
                field_id=column.key,
                name=column.key,
                kind=ParamKind.KW_ONLY,
            ),
        )

    for relationship in relationships:
        if relationship.collection_class is not None and strip_alias(relationship.collection_class) is not list:
            continue  # it is not supported
        if relationship.uselist is None:
            continue  # it cannot be None there

        fields.append(
            InputField(
                id=relationship.key,
                type=_get_type_for_relationship(relationship, type_hints),
                default=NoDefault(),
                is_required=False,
                metadata={},
                original=relationship,
            ),
        )
        params.append(
            Param(
                field_id=relationship.key,
                name=relationship.key,
                kind=ParamKind.KW_ONLY,
            ),
        )

    return InputShape(
        constructor=tp,
        fields=tuple(fields),
        overriden_types=frozenset(),
        kwargs=None,
        params=tuple(params),
    )


def _get_output_shape(
    columns: "ColumnCollection",
    relationships: "ReadOnlyProperties[RelationshipProperty[Any]]",
    type_hints: Mapping[str, TypeHint],
) -> OutputShape:
    output_fields = [
        OutputField(
            id=column.name,
            type=_get_type_for_column(column, type_hints),
            default=_get_default(column.default),
            metadata=column.info,
            original=IdWrapper(column),
            accessor=create_attr_accessor(column.name, is_required=True),
        )
        for column in columns
        if isinstance(column, sqlalchemy.Column)
    ]
    for relationship in relationships:
        if relationship.collection_class is not None and strip_alias(relationship.collection_class) is not list:
            continue  # it is not supported
        if relationship.uselist is None:
            continue  # it cannot be None there

        output_fields.append(
            OutputField(
                id=relationship.key,
                type=_get_type_for_relationship(relationship, type_hints),
                default=NoDefault(),
                metadata={},
                original=relationship,
                accessor=create_attr_accessor(relationship.key, is_required=True),
            ),
        )
    return OutputShape(
        fields=tuple(output_fields),
        overriden_types=frozenset(),
    )


def get_sqlalchemy_shape(tp) -> FullShape:
    if not HAS_SUPPORTED_SQLALCHEMY_PKG:
        if not HAS_SQLALCHEMY_PKG:
            raise NoTargetPackageError(HAS_SQLALCHEMY_PKG)
        raise TooOldPackageError(HAS_SUPPORTED_SQLALCHEMY_PKG)

    try:
        mapper = sqlalchemy.inspect(tp)
    except NoInspectionAvailable:
        raise IntrospectionError

    if not isinstance(mapper, Mapper):
        raise IntrospectionError

    if not isinstance(mapper.local_table, Table):
        raise ClarifiedIntrospectionError(
            "Only sqlalchemy mapping to Table is supported",
        )

    type_hints = get_all_type_hints(tp)
    return Shape(
        input=_get_input_shape(
            tp=tp,
            table=mapper.local_table,
            columns=mapper.columns,
            relationships=mapper.relationships,
            type_hints=type_hints,
        ),
        output=_get_output_shape(
            columns=mapper.columns,
            relationships=mapper.relationships,
            type_hints=type_hints,
        ),
    )
