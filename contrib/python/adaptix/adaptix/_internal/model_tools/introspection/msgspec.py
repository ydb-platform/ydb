import inspect
from collections.abc import Mapping
from types import MappingProxyType

try:
    from msgspec import NODEFAULT
    from msgspec.structs import FieldInfo, fields
except ImportError:
    pass
from ...feature_requirement import HAS_MSGSPEC_PKG, HAS_SUPPORTED_MSGSPEC_PKG
from ...type_tools import get_all_type_hints, is_class_var, normalize_type
from ..definitions import (
    Default,
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
    TooOldPackageError,
    create_attr_accessor,
)


def _get_default_from_field_info(fi: "FieldInfo") -> Default:
    if fi.default is not NODEFAULT:
        return DefaultValue(fi.default)
    if fi.default_factory is not NODEFAULT:
        return DefaultFactory(fi.default_factory)
    return NoDefault()


def _create_input_field_from_structs_field_info(fi: "FieldInfo", type_hints: Mapping[str, type]) -> InputField:
    default = _get_default_from_field_info(fi)
    return InputField(
        id=fi.name,
        type=type_hints[fi.name],
        default=default,
        is_required=fi.required,
        original=fi,
        metadata=MappingProxyType({}),
    )


def _get_kind_from_sig(param_kind):
    if param_kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
        return ParamKind.POS_OR_KW
    if param_kind == inspect.Parameter.KEYWORD_ONLY:
        return ParamKind.KW_ONLY
    if param_kind == inspect.Parameter.POSITIONAL_ONLY:
        return ParamKind.POS_ONLY
    raise IntrospectionError


def get_msgspec_shape(tp) -> FullShape:
    if not HAS_SUPPORTED_MSGSPEC_PKG:
        if not HAS_MSGSPEC_PKG:
            raise NoTargetPackageError(HAS_MSGSPEC_PKG)
        raise TooOldPackageError(HAS_SUPPORTED_MSGSPEC_PKG)

    try:
        fields_info = fields(tp)
    except TypeError:
        raise IntrospectionError

    type_hints = get_all_type_hints(tp)
    init_fields = tuple(
        field_name
        for field_name in type_hints
        if not is_class_var(normalize_type(type_hints[field_name]))
    )
    param_sig = inspect.signature(tp).parameters
    annotations = getattr(tp, "__annotations__", {})
    return FullShape(
        InputShape(
            constructor=tp,
            fields=tuple(
                _create_input_field_from_structs_field_info(fi, type_hints)
                for fi in fields_info if fi.name in init_fields
            ),
            params=tuple(
                Param(
                    field_id=field_id,
                    name=field_id,
                    kind=_get_kind_from_sig(param_sig[field_id].kind),
                )
                for field_id in type_hints
                if field_id in init_fields
            ),
            kwargs=None,
            overriden_types=frozenset(
                annotation
                for annotation in annotations
                if annotation in init_fields
            ),
        ),
        OutputShape(
            fields=tuple(
                OutputField(
                    id=fi.name,
                    type=type_hints[fi.name],
                    original=fi,
                    metadata=MappingProxyType({}),
                    default=_get_default_from_field_info(fi),
                    accessor=create_attr_accessor(attr_name=fi.name, is_required=True),
                ) for fi in fields_info),
            overriden_types=frozenset(
                annotation
                for annotation in annotations
                if annotation in init_fields
            ),
        ),
    )
