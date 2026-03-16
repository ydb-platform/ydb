import inspect
from dataclasses import MISSING as DC_MISSING, Field as DCField, fields as dc_fields, is_dataclass

from ...feature_requirement import HAS_PY_310
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
    OutputField,
    OutputShape,
    Param,
    ParamKind,
    Shape,
    create_attr_accessor,
)


def all_dc_fields(cls) -> dict[str, DCField]:
    """Builtin introspection function hides
    some fields like InitVar or ClassVar.
    That function returns full dict
    """
    return cls.__dataclass_fields__


def _get_default(field: DCField) -> Default:
    if field.default is not DC_MISSING:
        return DefaultValue(field.default)
    if field.default_factory is not DC_MISSING:
        return DefaultFactory(field.default_factory)
    return NoDefault()


def _create_inp_field_from_dc_field(dc_field: DCField, type_hints):
    default = _get_default(dc_field)
    return InputField(
        type=type_hints[dc_field.name],
        id=dc_field.name,
        default=default,
        is_required=default == NoDefault(),
        metadata=dc_field.metadata,
        original=dc_field,
    )


if HAS_PY_310:
    def _get_param_kind(dc_field: DCField) -> ParamKind:
        return ParamKind.KW_ONLY if dc_field.kw_only else ParamKind.POS_OR_KW
else:
    def _get_param_kind(dc_field: DCField) -> ParamKind:
        return ParamKind.POS_OR_KW


def get_dataclass_shape(tp) -> FullShape:
    """This function does not work properly if __init__ signature differs from
    that would be created by dataclass decorator.

    It happens because we cannot distinguish __init__ that generated
    by @dataclass and __init__ that created in other ways.
    And we cannot analyze only __init__ signature
    because @dataclass uses private constant
    as default value for fields with default_factory
    """

    if not is_dataclass(tp):
        raise IntrospectionError

    name_to_dc_field = all_dc_fields(tp)
    dc_fields_public = dc_fields(tp)
    init_params = list(
        inspect.signature(tp.__init__).parameters.keys(),  # type: ignore[misc]
    )[1:]
    type_hints = get_all_type_hints(tp)

    return Shape(
        input=InputShape(
            constructor=tp,  # type: ignore[arg-type]
            fields=tuple(
                _create_inp_field_from_dc_field(dc_field, type_hints)
                for dc_field in name_to_dc_field.values()
                if dc_field.init and not is_class_var(normalize_type(type_hints[dc_field.name]))
            ),
            params=tuple(
                Param(
                    field_id=field_id,
                    name=field_id,
                    kind=_get_param_kind(name_to_dc_field[field_id]),
                )
                for field_id in init_params
            ),
            kwargs=None,
            overriden_types=frozenset(
                field_id for field_id in init_params
                if field_id in tp.__annotations__
            ),
        ),
        output=OutputShape(
            fields=tuple(
                OutputField(
                    type=type_hints[dc_field.name],
                    id=dc_field.name,
                    default=_get_default(name_to_dc_field[dc_field.name]),
                    accessor=create_attr_accessor(attr_name=dc_field.name, is_required=True),
                    metadata=dc_field.metadata,
                    original=dc_field,
                )
                for dc_field in dc_fields_public
            ),
            overriden_types=frozenset(
                dc_field.name for dc_field in dc_fields_public
                if dc_field.name in tp.__annotations__
            ),
        ),
    )
