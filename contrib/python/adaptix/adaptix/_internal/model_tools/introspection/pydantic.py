import inspect
import itertools
from collections.abc import Sequence
from functools import cached_property
from inspect import Parameter, Signature
from typing import Annotated, Any

try:
    from pydantic import AliasChoices, BaseModel
    from pydantic.fields import ComputedFieldInfo, FieldInfo
    from pydantic_core import PydanticUndefined
except ImportError:
    pass

from adaptix import TypeHint

from ...feature_requirement import HAS_PYDANTIC_PKG, HAS_SUPPORTED_PYDANTIC_PKG
from ...type_tools import get_all_type_hints, is_pydantic_class
from ..definitions import (
    ClarifiedIntrospectionError,
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
    ParamKwargs,
    Shape,
    TooOldPackageError,
    create_attr_accessor,
)


def _get_default(field) -> Default:
    if field.default_factory is not None:
        return DefaultFactory(field.default_factory)
    if field.default is PydanticUndefined:
        return NoDefault()
    return DefaultValue(field.default)


_config_defaults = {
    "populate_by_name": False,
    "extra": "ignore",
}


def _get_config_value(tp: "type[BaseModel]", key: str) -> Any:
    try:
        return tp.model_config[key]  # type: ignore[literal-required]
    except KeyError:
        pass

    return _config_defaults[key]


def _get_field_parameters(tp: "type[BaseModel]", field_name: str, field_info: "FieldInfo") -> Sequence[str]:
    # AliasPath is ignored
    if field_info.validation_alias is None:
        parameters = [field_name]
    else:
        parameters = [field_name] if _get_config_value(tp, "populate_by_name") else []
        if isinstance(field_info.validation_alias, str):
            parameters.append(field_info.validation_alias)
        elif isinstance(field_info.validation_alias, AliasChoices):
            parameters.extend(alias for alias in field_info.validation_alias.choices if isinstance(alias, str))
    return [param for param in parameters if param.isidentifier()]


def _get_field_parameter_name(tp: "type[BaseModel]", field_name: str, field_info: "FieldInfo") -> str:
    parameters = _get_field_parameters(tp, field_name, field_info)
    if not parameters:
        raise ClarifiedIntrospectionError(
            f"Cannot fetch parameter name for field {field_name!r}."
            f" This means that field has only AliasPath aliases or non-python-identifier aliases"
            f" and populate_by_name is disabled",
        )
    return parameters[0]


def _signature_is_self_with_kwargs_only(init_signature: Signature) -> bool:
    try:
        self, kwargs = init_signature.parameters.values()
    except ValueError:
        return False
    return (
        self.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
        and kwargs.kind == Parameter.VAR_KEYWORD
    )


def _get_field_type(field_info: "FieldInfo") -> TypeHint:
    if field_info.metadata:
        return Annotated[(field_info.annotation, *field_info.metadata)]
    return field_info.annotation


def _get_input_shape(tp: "type[BaseModel]") -> InputShape:
    if not _signature_is_self_with_kwargs_only(inspect.signature(tp.__init__)):
        raise ClarifiedIntrospectionError(
            "Pydantic model `__init__` must takes only self and one variable keyword parameter",
        )

    return InputShape(
        constructor=tp,
        fields=tuple(
            InputField(
                id=field_id,
                type=_get_field_type(field_info),
                default=_get_default(field_info),
                metadata={},  # pydantic metadata is the list
                original=field_info,
                is_required=_get_default(field_info) == NoDefault(),
            )
            for field_id, field_info in tp.model_fields.items()
        ),
        overriden_types=frozenset(
            field_id for field_id in tp.model_fields
            if field_id in tp.__annotations__
        ),
        params=tuple(
            Param(
                field_id=field_id,
                kind=ParamKind.KW_ONLY,
                name=_get_field_parameter_name(tp, field_id, field_info),
            )
            for field_id, field_info in tp.model_fields.items()
        ),
        kwargs=None if _get_config_value(tp, "extra") == "forbid" else ParamKwargs(Any),
    )


def _unwrap_getter_function(descriptor, field_id: str):
    if isinstance(descriptor, property):
        if descriptor.fget is None:
            raise ClarifiedIntrospectionError(f"Computed field {field_id!r} has no getter")
        return descriptor.fget
    if isinstance(descriptor, cached_property):
        return descriptor.func
    raise ClarifiedIntrospectionError(f"Computed field {field_id!r} has unknown descriptor {descriptor}")


def _get_computed_field_type(field_id: str, computed_field_info: "ComputedFieldInfo") -> TypeHint:
    if computed_field_info.return_type is not PydanticUndefined:
        return computed_field_info.return_type

    getter_function = _unwrap_getter_function(computed_field_info.wrapped_property, field_id)
    signature = inspect.signature(getter_function)
    if signature.return_annotation is inspect.Signature.empty:
        return Any
    return signature.return_annotation


def _get_output_shape(tp: "type[BaseModel]") -> OutputShape:
    type_hints = get_all_type_hints(tp)
    fields = itertools.chain(
        (
            OutputField(
                id=field_id,
                type=_get_field_type(field_info),
                default=_get_default(field_info),
                metadata={},  # pydantic metadata is the list
                original=field_info,
                accessor=create_attr_accessor(field_id, is_required=True),
            )
            for field_id, field_info in tp.model_fields.items()
        ),
        (
            OutputField(
                id=field_id,
                type=_get_computed_field_type(field_id, computed_field_dec.info),
                default=NoDefault(),
                metadata={},
                original=computed_field_dec.info,
                accessor=create_attr_accessor(field_id, is_required=True),
            )
            for field_id, computed_field_dec in tp.__pydantic_decorators__.computed_fields.items()
        ),
        (
            OutputField(
                id=field_id,
                type=type_hints.get(field_id, Any),
                default=_get_default(private_attr),
                metadata={},
                original=private_attr,
                accessor=create_attr_accessor(field_id, is_required=True),
            )
            for field_id, private_attr in tp.__private_attributes__.items()
        ),
    )
    return OutputShape(
        fields=tuple(fields),
        overriden_types=frozenset(
            itertools.chain(
                (
                    field_id for field_id in tp.model_fields
                    if field_id in tp.__annotations__
                ),
                (
                    field_id for field_id in tp.__pydantic_decorators__.computed_fields
                    if field_id in tp.__dict__
                ),
                (
                    field_id for field_id in tp.__private_attributes__
                    if field_id in tp.__annotations__ or field_id not in type_hints
                ),
            ),
        ),
    )


def get_pydantic_shape(tp) -> FullShape:
    if not HAS_SUPPORTED_PYDANTIC_PKG:
        if not HAS_PYDANTIC_PKG:
            raise NoTargetPackageError(HAS_PYDANTIC_PKG)
        raise TooOldPackageError(HAS_SUPPORTED_PYDANTIC_PKG)

    if not is_pydantic_class(tp):
        raise IntrospectionError

    return Shape(
        input=_get_input_shape(tp),
        output=_get_output_shape(tp),
    )
