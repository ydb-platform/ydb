from typing import Any

from ...feature_requirement import HAS_ATTRS_PKG, HAS_SUPPORTED_ATTRS_PKG
from ...type_tools import get_all_type_hints
from ..definitions import (
    Default,
    DefaultFactory,
    DefaultFactoryWithSelf,
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
    Shape,
    TooOldPackageError,
    create_attr_accessor,
)
from .class_init import get_class_init_shape

try:
    import attrs
except ImportError:
    attrs = None  # type: ignore[assignment]


def _get_default(attrs_field) -> Default:
    default: Any = attrs_field.default

    if isinstance(default, attrs.Factory):  # type: ignore[arg-type]
        if default.takes_self:
            return DefaultFactoryWithSelf(default.factory)
        return DefaultFactory(default.factory)

    if default is attrs.NOTHING:
        return NoDefault()

    return DefaultValue(default)


def _get_field_type(attrs_field, type_hints):
    try:
        return type_hints[attrs_field.name]
    except KeyError:
        return Any if attrs_field.type is None else attrs_field.type


def _get_param_name(attrs_field):
    if hasattr(attrs_field, "alias"):
        return attrs_field.alias
    return (
        attrs_field.name[1:]
        if attrs_field.name.startswith("_") and not attrs_field.name.startswith("__") else
        attrs_field.name
    )


def _get_input_shape(tp, attrs_fields, type_hints) -> InputShape:
    param_name_to_field_from_attrs = {
        _get_param_name(attrs_fld): InputField(
            id=attrs_fld.name,
            type=_get_field_type(attrs_fld, type_hints),
            default=_get_default(attrs_fld),
            metadata=attrs_fld.metadata,
            original=attrs_fld,
            is_required=_get_default(attrs_fld) == NoDefault(),
        )
        for attrs_fld in attrs_fields
        if attrs_fld.init
    }
    init_shape = get_class_init_shape(tp)

    if hasattr(tp, "__attrs_init__"):
        fields = tuple(
            InputField(
                id=param_name_to_field_from_attrs[fld.id].id,
                type=fld.type,
                default=fld.default,
                is_required=fld.is_required,
                metadata=param_name_to_field_from_attrs[fld.id].metadata,
                original=param_name_to_field_from_attrs[fld.id].original,
            )
            if fld.id in param_name_to_field_from_attrs else
            fld
            for fld in init_shape.input.fields
        )
        overriden_types = (
            frozenset(fld.id for fld in fields)
            if "__attrs_init__" in vars(tp) else
            frozenset()
        )
    else:
        fields = tuple(param_name_to_field_from_attrs.values())
        overriden_types = frozenset(
            attrs_fld.name
            for attrs_fld in attrs_fields
            if not attrs_fld.inherited and attrs_fld.init
        )

    return InputShape(
        constructor=tp,
        fields=fields,
        overriden_types=overriden_types,
        kwargs=init_shape.input.kwargs,
        params=tuple(
            Param(
                field_id=(
                    param_name_to_field_from_attrs[param.name].id
                    if param.name in param_name_to_field_from_attrs else
                    param.name
                ),
                name=param.name,
                kind=param.kind,
            )
            for param in init_shape.input.params
        ),
    )


def _get_output_shape(attrs_fields, type_hints) -> OutputShape:
    output_fields = tuple(
        OutputField(
            id=attrs_fld.name,
            type=_get_field_type(attrs_fld, type_hints),
            default=_get_default(attrs_fld),
            metadata=attrs_fld.metadata,
            original=attrs_fld,
            accessor=create_attr_accessor(attrs_fld.name, is_required=True),
        )
        for attrs_fld in attrs_fields
    )
    return OutputShape(
        fields=output_fields,
        overriden_types=frozenset(
            attrs_fld.name for attrs_fld in attrs_fields if not attrs_fld.inherited
        ),
    )


def get_attrs_shape(tp) -> FullShape:
    if not HAS_SUPPORTED_ATTRS_PKG:
        if not HAS_ATTRS_PKG:
            raise NoTargetPackageError(HAS_ATTRS_PKG)
        raise TooOldPackageError(HAS_SUPPORTED_ATTRS_PKG)

    try:
        is_attrs = attrs.has(tp)
    except TypeError:
        raise IntrospectionError
    if not is_attrs:
        raise IntrospectionError

    try:
        attrs_fields = attrs.fields(tp)
    except (TypeError, attrs.exceptions.NotAnAttrsClassError):
        raise IntrospectionError

    type_hints = get_all_type_hints(tp)
    return Shape(
        input=_get_input_shape(tp, attrs_fields, type_hints),
        output=_get_output_shape(attrs_fields, type_hints),
    )
