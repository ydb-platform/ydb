from __future__ import annotations

import ast
import dataclasses
import inspect
import operator
from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from copy import copy
from enum import Enum
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

from ._imports import PydanticUndefined, PydanticV1Undefined, attr
from .utils import _is_pydantic_model

if TYPE_CHECKING:
    from types import MappingProxyType

    from .adapter import AdapterInterface, ItemAdapter


SIMPLE_TYPES = {
    type(None): "null",
    bool: "boolean",
    int: "integer",
    float: "number",
    str: "string",
}


@dataclasses.dataclass
class _JsonSchemaState:
    adapter: type[ItemAdapter | AdapterInterface]
    """ItemAdapter class or AdapterInterface implementation used on the initial
    get_json_schema() call.

    On types for which adapter.is_item_class() returns True,
    adapter.get_json_schema() is used to get the corresponding, nested JSON
    Schema.
    """
    containers: set[type] = dataclasses.field(default_factory=set)
    """Used to keep track of item classes that are being processed, to avoid
    recursion."""


def dedupe_types(types: Sequence[type]) -> list[type]:
    seen = set()
    result = []
    for t in types:
        key = float if t in (int, float) else t
        if key not in seen:
            seen.add(key)
            result.append(t)
    return result


def update_prop_from_union(prop: dict[str, Any], prop_type: Any, state: _JsonSchemaState) -> None:
    prop_types = dedupe_types(get_args(prop_type))
    simple_types = [v for k, v in SIMPLE_TYPES.items() if k in prop_types]
    complex_types = sorted([t for t in prop_types if t not in SIMPLE_TYPES])  # type: ignore[type-var]
    if not complex_types:
        prop.setdefault("type", simple_types)
        return
    new_any_of: list[dict[str, Any]] = []
    any_of = prop.setdefault("anyOf", new_any_of)
    if any_of is not new_any_of:
        return
    any_of.append({"type": simple_types if len(simple_types) > 1 else simple_types[0]})
    for complex_type in complex_types:
        complex_prop: dict[str, Any] = {}
        update_prop_from_type(complex_prop, complex_type, state)
        any_of.append(complex_prop)


@runtime_checkable
class ArrayProtocol(Protocol):
    def __iter__(self) -> Iterator[Any]: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any) -> bool: ...


@runtime_checkable
class ObjectProtocol(Protocol):  # noqa: PLW1641
    def __getitem__(self, key: str) -> Any: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: str) -> bool: ...
    def keys(self): ...
    def items(self): ...
    def values(self): ...
    def get(self, key: str, default: Any = ...): ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...


INVALID_PATTERN_SUBSTRINGS = [
    "(?P<",  # named groups
    "(?<=",  # lookbehind
    "(?<!",  # negative lookbehind
    "(?>",  # atomic group
    "\\A",  # start of string
    "\\Z",  # end of string
    "(?i)",  # inline flags (case-insensitive, etc.)
    "(?m)",  # multiline
    "(?s)",  # dotall
    "(?x)",  # verbose
    "(?#",  # comments
]


def is_valid_pattern(pattern: str) -> bool:
    # https://ecma-international.org/publications-and-standards/standards/ecma-262/
    #
    # Note: We allow word boundaries (\b, \B) in patterns even thought there is
    # a difference in behavior: in Python, they work with Unicode; in JSON
    # Schema, they only work with ASCII.
    return not any(sub in pattern for sub in INVALID_PATTERN_SUBSTRINGS)


def array_type(type_hint):
    """Given the type hint of a Python type that maps to a JSON Schema array,
    such as a list, a tuple or a set, return the type of the items in that
    array."""
    args = get_args(type_hint)
    if not args:
        return Any
    if args[-1] is Ellipsis:
        args = args[:-1]
    unique_args = set(args)
    if len(unique_args) == 1:
        return next(iter(unique_args))
    return Union[tuple(unique_args)]


def update_prop_from_pattern(prop: dict[str, Any], pattern: str) -> None:
    if is_valid_pattern(pattern):
        prop.setdefault("pattern", pattern)


try:
    from types import UnionType
except ImportError:  # Python < 3.10
    UNION_TYPES: set[Any] = {Union}
else:
    UNION_TYPES = {Union, UnionType}


def update_prop_from_origin(
    prop: dict[str, Any], origin: Any, prop_type: Any, state: _JsonSchemaState
) -> None:
    if isinstance(origin, type):
        if issubclass(origin, (Sequence, AbstractSet)):
            prop.setdefault("type", "array")
            if issubclass(origin, AbstractSet):
                prop.setdefault("uniqueItems", True)
            had_items = "items" in prop
            items = prop.setdefault("items", {})
            item_type = array_type(prop_type)
            update_prop_from_type(items, item_type, state)
            if not items and not had_items:
                del prop["items"]
            return
        if issubclass(origin, Mapping):
            prop.setdefault("type", "object")
            args = get_args(prop_type)
            if args:
                assert len(args) == 2
                value_type = args[1]
                props = prop.setdefault("additionalProperties", {})
                update_prop_from_type(props, value_type, state)
            return
    if origin in UNION_TYPES:
        update_prop_from_union(prop, prop_type, state)


def update_prop_from_type(prop: dict[str, Any], prop_type: Any, state: _JsonSchemaState) -> None:
    if (origin := get_origin(prop_type)) is not None:
        update_prop_from_origin(prop, origin, prop_type, state)
        return
    if isinstance(prop_type, type):
        if state.adapter.is_item_class(prop_type):
            if prop_type in state.containers:
                prop.setdefault("type", "object")
                return
            state.containers.add(prop_type)
            subschema = state.adapter.get_json_schema(
                prop_type,
                _state=state,
            )
            state.containers.remove(prop_type)
            for k, v in subschema.items():
                prop.setdefault(k, v)
            return
        if issubclass(prop_type, Enum):
            values = [item.value for item in prop_type]
            value_types = tuple({type(v) for v in values})
            prop_type = value_types[0] if len(value_types) == 1 else Union[value_types]
            update_prop_from_type(prop, prop_type, state)
            prop.setdefault("enum", values)
            return
        if not issubclass(prop_type, str):
            if isinstance(prop_type, ObjectProtocol):
                prop.setdefault("type", "object")
                return
            if isinstance(prop_type, ArrayProtocol):
                prop.setdefault("type", "array")
                if issubclass(prop_type, AbstractSet):
                    prop.setdefault("uniqueItems", True)
                return
    json_schema_type = SIMPLE_TYPES.get(prop_type)
    if json_schema_type is not None:
        prop.setdefault("type", json_schema_type)


def _setdefault_attribute_types_on_json_schema(
    schema: dict[str, Any], item_class: type, state: _JsonSchemaState
) -> None:
    """Inspect the type hints of the class attributes of the item class and,
    for any matching JSON Schema property that has no type set, set the type
    based on the type hint."""
    props = schema.get("properties", {})
    attribute_type_hints = get_type_hints(item_class)
    for prop_name, prop in props.items():
        if prop_name not in attribute_type_hints:
            continue
        prop_type = attribute_type_hints[prop_name]
        update_prop_from_type(prop, prop_type, state)


def iter_docstrings(item_class: type, attr_names: AbstractSet[str]) -> Iterator[tuple[str, str]]:
    try:
        source = inspect.getsource(item_class)
    except (OSError, TypeError):
        return
    tree = ast.parse(dedent(source))
    class_node = None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == item_class.__name__:
            class_node = node
            break
    if class_node is None:  # pragma: no cover
        # This can be reproduced with the doctests of the README, but the
        # coverage data does not seem to include those.
        return
    assert isinstance(class_node, ast.ClassDef)
    for child in ast.iter_child_nodes(class_node):
        if isinstance(child, ast.Assign) and isinstance(child.targets[0], ast.Name):
            attr_name = child.targets[0].id
        elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            attr_name = child.target.id
        else:
            continue
        if attr_name not in attr_names:
            continue
        next_idx = class_node.body.index(child) + 1
        if next_idx >= len(class_node.body):
            continue
        next_node = class_node.body[next_idx]
        if (
            isinstance(next_node, ast.Expr)
            and isinstance(next_node.value, ast.Constant)
            and isinstance(next_node.value.value, str)
        ):
            yield attr_name, next_node.value.value


def get_inherited_attr_docstring(item_class: type, attr_name: str) -> str | None:
    """Recursively search the MRO for a docstring for the given attribute
    name."""
    for cls in item_class.__mro__:
        for name, doc in iter_docstrings(cls, {attr_name}):
            if name == attr_name:
                return doc
    return None


def _setdefault_attribute_docstrings_on_json_schema(
    schema: dict[str, Any], item_class: type
) -> None:
    """Inspect the docstrings after each class attribute of the item class and
    its bases and, for any matching JSON Schema property that has no
    description set, set the description to the contents of the docstring."""
    props = schema.get("properties", {})
    attr_names = set(props)
    if not attr_names:
        return
    for attr_name in attr_names:
        prop = props.setdefault(attr_name, {})
        if "description" not in prop:
            doc = get_inherited_attr_docstring(item_class, attr_name)
            if doc:
                prop["description"] = inspect.cleandoc(doc)


def base_json_schema_from_item_class(item_class: type) -> dict[str, Any]:
    json_schema_extra = getattr(item_class, "__json_schema_extra__", {})
    schema = copy(json_schema_extra)
    schema.setdefault("type", "object")
    schema.setdefault("additionalProperties", False)
    return schema


def _json_schema_from_item_class(
    adapter: type[AdapterInterface], item_class: type, state: _JsonSchemaState | None = None
) -> dict[str, Any]:
    state = state or _JsonSchemaState(adapter=adapter, containers={item_class})
    schema = base_json_schema_from_item_class(item_class)
    fields_meta = {
        field_name: adapter.get_field_meta_from_class(item_class, field_name)
        for field_name in adapter.get_field_names_from_class(item_class) or ()
    }
    if not fields_meta:
        return schema
    schema["properties"] = {
        field_name: copy(field_meta.get("json_schema_extra", {}))
        for field_name, field_meta in fields_meta.items()
    }
    required = [
        field_name
        for field_name, field_data in schema["properties"].items()
        if "default" not in field_data
    ]
    if required:
        schema.setdefault("required", required)
    return schema


def update_required_fields(
    schema: dict[str, Any], optional_fields: set[str] | None = None
) -> None:
    optional_fields = optional_fields or set()
    if "required" in schema:
        return
    required = [
        field
        for field, metadata in schema["properties"].items()
        if field not in optional_fields and "default" not in metadata
    ]
    if required:
        schema["required"] = required


def _json_schema_from_attrs(item_class: type, state: _JsonSchemaState) -> dict[str, Any]:
    schema = base_json_schema_from_item_class(item_class)
    fields = attr.fields(item_class)
    if not fields:
        return schema

    from attr import resolve_types

    resolve_types(item_class)  # Ensure field.type annotations are resolved

    schema["properties"] = {
        field.name: copy(field.metadata.get("json_schema_extra", {})) for field in fields
    }
    default_factory_fields: set[str] = set()
    for field in fields:
        prop = schema["properties"][field.name]
        _update_attrs_prop(prop, field, state, default_factory_fields)
    update_required_fields(schema, default_factory_fields)
    _setdefault_attribute_docstrings_on_json_schema(schema, item_class)
    return schema


def _update_attrs_prop(
    prop: dict[str, Any],
    field: attr.Attribute,
    state: _JsonSchemaState,
    default_factory_fields: set[str],
) -> None:
    update_prop_from_type(prop, field.type, state)
    if isinstance(field.default, attr.Factory):
        default_factory_fields.add(field.name)
    elif field.default is not attr.NOTHING:
        prop.setdefault("default", field.default)
    _update_attrs_prop_validation(prop, field)


ATTRS_NUMBER_VALIDATORS = {
    operator.ge: "minimum",
    operator.gt: "exclusiveMinimum",
    operator.le: "maximum",
    operator.lt: "exclusiveMaximum",
}


def _update_attrs_prop_validation(
    prop: dict[str, Any],
    field: attr.Attribute,
) -> None:
    if not field.validator:
        return
    if type(field.validator).__name__ == "_AndValidator":
        validators = field.validator._validators
    else:
        validators = [field.validator]
    for validator in validators:
        validator_type_name = type(validator).__name__
        if validator_type_name == "_NumberValidator":
            key = ATTRS_NUMBER_VALIDATORS.get(validator.compare_func)
            if not key:  # pragma: no cover
                continue
            prop.setdefault(key, validator.bound)
        elif validator_type_name == "_InValidator":
            prop.setdefault("enum", list(validator.options))
        elif validator_type_name == "_MinLengthValidator":
            key = "minLength" if field.type is str else "minItems"
            prop.setdefault(key, validator.min_length)
        elif validator_type_name == "_MaxLengthValidator":
            key = "maxLength" if field.type is str else "maxItems"
            prop.setdefault(key, validator.max_length)
        elif validator_type_name == "_MatchesReValidator":
            pattern_obj = getattr(validator, "pattern", None) or validator.regex
            update_prop_from_pattern(prop, pattern_obj.pattern)


def _json_schema_from_dataclass(item_class: type, state: _JsonSchemaState) -> dict[str, Any]:
    schema = base_json_schema_from_item_class(item_class)
    fields = dataclasses.fields(item_class)
    resolved_field_types = get_type_hints(item_class)
    default_factory_fields = set()
    if fields:
        schema["properties"] = {
            field.name: copy(field.metadata.get("json_schema_extra", {})) for field in fields
        }
        for field in fields:
            prop = schema["properties"][field.name]
            field_type = resolved_field_types.get(field.name)
            if field_type is not None:
                update_prop_from_type(prop, field_type, state)
            if field.default_factory is not dataclasses.MISSING:
                default_factory_fields.add(field.name)
            elif field.default is not dataclasses.MISSING:
                prop.setdefault("default", field.default)
        update_required_fields(schema, default_factory_fields)
    _setdefault_attribute_docstrings_on_json_schema(schema, item_class)
    return schema


def _json_schema_from_pydantic(
    adapter: type[AdapterInterface], item_class: type, state: _JsonSchemaState | None = None
) -> dict[str, Any]:
    state = state or _JsonSchemaState(adapter=adapter, containers={item_class})
    if not _is_pydantic_model(item_class):
        return _json_schema_from_pydantic_v1(adapter, item_class, state)
    schema = copy(
        item_class.model_config.get("json_schema_extra", {})  # type: ignore[attr-defined]
    )
    extra = item_class.model_config.get("extra")  # type: ignore[attr-defined]
    schema.setdefault("type", "object")
    if extra == "forbid":
        schema.setdefault("additionalProperties", False)
    fields = {
        name: adapter.get_field_meta_from_class(item_class, name)
        for name in adapter.get_field_names_from_class(item_class) or ()
    }
    if not fields:
        return schema
    schema["properties"] = {
        name: copy(metadata.get("json_schema_extra", {})) for name, metadata in fields.items()
    }
    default_factory_fields: set[str] = set()
    for name, metadata in fields.items():
        prop = schema["properties"][name]
        _update_pydantic_prop(prop, name, metadata, state, default_factory_fields)
    update_required_fields(schema, default_factory_fields)
    _setdefault_attribute_docstrings_on_json_schema(schema, item_class)
    return schema


def _update_pydantic_prop(
    prop: dict[str, Any],
    name: str,
    metadata: MappingProxyType,
    _state: _JsonSchemaState,
    default_factory_fields: set[str],
) -> None:
    if "annotation" in metadata:
        field_type = metadata["annotation"]
        if field_type is not None:
            update_prop_from_type(prop, field_type, _state)
    if "default_factory" in metadata:
        default_factory_fields.add(name)
    elif "default" in metadata and metadata["default"] is not PydanticUndefined:
        prop.setdefault("default", metadata["default"])
    if "metadata" in metadata:
        _update_pydantic_prop_validation(prop, metadata["metadata"], field_type)
    for metadata_key, json_schema_field in (
        ("title", "title"),
        ("description", "description"),
        ("examples", "examples"),
    ):
        if metadata_key in metadata:
            prop.setdefault(json_schema_field, metadata[metadata_key])
    if "deprecated" in metadata:
        prop.setdefault("deprecated", bool(metadata["deprecated"]))


def _update_pydantic_prop_validation(
    prop: dict[str, Any],
    metadata: Sequence[Any],
    field_type: type,
) -> None:
    for metadata_item in metadata:
        metadata_item_type = type(metadata_item).__name__
        if metadata_item_type == "_PydanticGeneralMetadata":
            if "pattern" in metadata_item.__dict__:
                pattern = metadata_item.__dict__["pattern"]
                update_prop_from_pattern(prop, pattern)
        elif metadata_item_type == "MinLen":
            key = "minLength" if field_type is str else "minItems"
            prop.setdefault(key, metadata_item.min_length)
        elif metadata_item_type == "MaxLen":
            key = "maxLength" if field_type is str else "maxItems"
            prop.setdefault(key, metadata_item.max_length)
        else:
            for metadata_key, json_schema_field in (
                ("ge", "minimum"),
                ("gt", "exclusiveMinimum"),
                ("le", "maximum"),
                ("lt", "exclusiveMaximum"),
            ):
                if metadata_item_type == metadata_key.capitalize():
                    prop.setdefault(json_schema_field, getattr(metadata_item, metadata_key))


def _json_schema_from_pydantic_v1(
    adapter: type[AdapterInterface], item_class: type, state: _JsonSchemaState
) -> dict[str, Any]:
    schema = copy(
        getattr(item_class.Config, "schema_extra", {})  # type: ignore[attr-defined]
    )
    extra = getattr(item_class.Config, "extra", None)  # type: ignore[attr-defined]
    schema.setdefault("type", "object")
    if extra == "forbid":
        schema.setdefault("additionalProperties", False)
    fields = {
        name: adapter.get_field_meta_from_class(item_class, name)
        for name in adapter.get_field_names_from_class(item_class) or ()
    }
    if not fields:
        return schema
    schema["properties"] = {
        name: copy(metadata.get("json_schema_extra", {})) for name, metadata in fields.items()
    }
    default_factory_fields: set[str] = set()
    field_type_hints = get_type_hints(item_class)
    for name, metadata in fields.items():
        prop = schema["properties"][name]
        _update_pydantic_v1_prop(
            prop, name, metadata, field_type_hints, default_factory_fields, state
        )
    update_required_fields(schema, default_factory_fields)
    _setdefault_attribute_docstrings_on_json_schema(schema, item_class)
    return schema


def _update_pydantic_v1_prop(  # pylint: disable=too-many-positional-arguments,too-many-arguments
    prop: dict[str, Any],
    name: str,
    metadata: Mapping[str, Any],
    field_type_hints: dict[str, Any],
    default_factory_fields: set[str],
    state: _JsonSchemaState,
) -> None:
    field_type = field_type_hints[name]
    if field_type is not None:
        update_prop_from_type(prop, field_type, state)
    if "default_factory" in metadata:
        default_factory_fields.add(name)
    elif "default" in metadata and metadata["default"] not in (
        Ellipsis,
        PydanticV1Undefined,
    ):
        prop.setdefault("default", metadata["default"])
    for metadata_key, json_schema_field in (
        ("title", "title"),
        ("description", "description"),
        ("examples", "examples"),
        ("ge", "minimum"),
        ("gt", "exclusiveMinimum"),
        ("le", "maximum"),
        ("lt", "exclusiveMaximum"),
    ):
        if metadata_key in metadata:
            prop.setdefault(json_schema_field, metadata[metadata_key])
    for prefix in ("min", "max"):
        if f"{prefix}_length" in metadata:
            key = f"{prefix}Length" if field_type is str else f"{prefix}Items"
            prop.setdefault(key, metadata[f"{prefix}_length"])
        elif f"{prefix}_items" in metadata:
            prop.setdefault(f"{prefix}Items", metadata[f"{prefix}_items"])
    for metadata_key in ("pattern", "regex"):
        if metadata_key in metadata:
            pattern = metadata[metadata_key]
            update_prop_from_pattern(prop, pattern)
            break
    if "deprecated" in metadata:
        prop.setdefault("deprecated", bool(metadata["deprecated"]))
