from __future__ import annotations

from types import MappingProxyType
from typing import Any

from itemadapter._imports import (
    PydanticUndefined,
    PydanticV1Undefined,
    attr,
    pydantic,
    pydantic_v1,
)

__all__ = ["get_field_meta_from_class", "is_item"]


def _is_attrs_class(obj: Any) -> bool:
    if attr is None:
        return False
    return attr.has(obj)


def _is_pydantic_model(obj: Any) -> bool:
    if pydantic is None:
        return False
    return issubclass(obj, pydantic.BaseModel)


def _is_pydantic_v1_model(obj: Any) -> bool:
    if pydantic_v1 is None:
        return False
    return issubclass(obj, pydantic_v1.BaseModel)


def _get_pydantic_model_metadata(item_model: Any, field_name: str) -> MappingProxyType:
    metadata = {}
    field = item_model.model_fields[field_name]

    for attribute in [
        "alias_priority",
        "alias",
        "allow_inf_nan",
        "annotation",
        "coerce_numbers_to_str",
        "decimal_places",
        "default_factory",
        "deprecated",
        "description",
        "discriminator",
        "examples",
        "exclude",
        "fail_fast",
        "field_title_generator",
        "frozen",
        "ge",
        "gt",
        "init_var",
        "init",
        "json_schema_extra",
        "kw_only",
        "le",
        "lt",
        "max_digits",
        "max_length",
        "min_length",
        "multiple_of",
        "pattern",
        "repr",
        "serialization_alias",
        "strict",
        "title",
        "union_mode",
        "validate_default",
        "validation_alias",
    ]:
        if hasattr(field, attribute) and (value := getattr(field, attribute)) is not None:
            metadata[attribute] = value

    for attribute, default_value in [
        ("default", PydanticUndefined),
        ("metadata", []),
    ]:
        if hasattr(field, attribute) and (value := getattr(field, attribute)) != default_value:
            metadata[attribute] = value

    return MappingProxyType(metadata)


def _get_pydantic_v1_model_metadata(item_model: Any, field_name: str) -> MappingProxyType:
    metadata = {}
    field = item_model.__fields__[field_name]
    field_info = field.field_info

    for attribute in [
        "alias",
        "const",
        "description",
        "ge",
        "gt",
        "le",
        "lt",
        "max_items",
        "max_length",
        "min_items",
        "min_length",
        "multiple_of",
        "regex",
        "title",
    ]:
        value = getattr(field_info, attribute)
        if value is not None:
            metadata[attribute] = value

    if (value := field_info.default) not in (PydanticV1Undefined, Ellipsis):
        metadata["default"] = value

    if value := field.default_factory is not None:
        metadata["default_factory"] = value

    if not field_info.allow_mutation:
        metadata["allow_mutation"] = field_info.allow_mutation
    metadata.update(field_info.extra)

    return MappingProxyType(metadata)


def is_item(obj: Any) -> bool:
    """Return True if the given object belongs to one of the supported types, False otherwise.

    Alias for ItemAdapter.is_item
    """
    from itemadapter.adapter import ItemAdapter

    return ItemAdapter.is_item(obj)


def get_field_meta_from_class(item_class: type, field_name: str) -> MappingProxyType:
    """Return a read-only mapping with metadata for the given field name, within the given
    item class. If there is no metadata for the field, or the item class does not support
    field metadata, an empty object is returned.

    Field metadata is taken from different sources, depending on the item type:
    * scrapy.item.Item: corresponding scrapy.item.Field object
    * dataclass items: "metadata" attribute for the corresponding field
    * attrs items: "metadata" attribute for the corresponding field
    * pydantic models: corresponding pydantic.field.FieldInfo/ModelField object

    The returned value is an instance of types.MappingProxyType, i.e. a dynamic read-only view
    of the original mapping, which gets automatically updated if the original mapping changes.
    """

    from itemadapter.adapter import ItemAdapter

    return ItemAdapter.get_field_meta_from_class(item_class, field_name)
