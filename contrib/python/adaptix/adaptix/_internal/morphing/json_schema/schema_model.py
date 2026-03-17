from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar, Union

from ...utils import Omittable, Omitted

T = TypeVar("T")

JSONNumeric = Union[int, float]
JSONObject = Mapping[str, T]

# Recursive normalized types are not supported now.
JSONValue = Union[
    JSONNumeric,
    str,
    bool,
    None,
    Sequence[Any],
    JSONObject[Any],
]


class JSONSchemaType(Enum):
    NULL = "null"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    NUMBER = "number"
    INTEGER = "integer"
    STRING = "string"

    def __repr__(self):
        return f"{type(self).__name__}.{self.name}"


class JSONSchemaBuiltinFormat(Enum):
    DATE_TIME = "date-time"
    DATE = "date"
    TIME = "time"
    DURATION = "duration"
    EMAIL = "email"
    IDN_EMAIL = "idn-email"
    HOSTNAME = "hostname"
    IDN_HOSTNAME = "idn-hostname"
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    URI = "uri"
    URI_REFERENCE = "uri-reference"
    IRI = "iri"
    IRI_REFERENCE = "iri-reference"
    UUID = "uuid"
    URI_TEMPLATE = "uri-template"
    JSON_POINTER = "json-pointer"
    RELATIVE_JSON_POINTER = "relative-json-pointer"
    REGEX = "regex"

    def __repr__(self):
        return f"{type(self).__name__}.{self.name}"


JSONSchemaT = TypeVar("JSONSchemaT")
RefT = TypeVar("RefT")


@dataclass
class _JSONSchemaCore(Generic[RefT, JSONSchemaT]):
    schema: Omittable[str] = Omitted()
    vocabulary: Omittable[JSONObject[bool]] = Omitted()
    id: Omittable[str] = Omitted()
    anchor: Omittable[str] = Omitted()
    dynamic_anchor: Omittable[str] = Omitted()
    ref: Omittable[RefT] = Omitted()
    dynamic_ref: Omittable[str] = Omitted()
    defs: Omittable[JSONObject[JSONSchemaT]] = Omitted()
    comment: Omittable[str] = Omitted()


@dataclass
class _JSONSchemaSubschemas(Generic[JSONSchemaT]):
    # combinators
    all_of: Omittable[Sequence[JSONSchemaT]] = Omitted()
    any_of: Omittable[Sequence[JSONSchemaT]] = Omitted()
    one_of: Omittable[Sequence[JSONSchemaT]] = Omitted()
    not_: Omittable[JSONSchemaT] = Omitted()

    # conditions
    if_: Omittable[JSONSchemaT] = Omitted()
    then: Omittable[JSONSchemaT] = Omitted()
    else_: Omittable[JSONSchemaT] = Omitted()
    dependent_schemas: Omittable[JSONObject[JSONSchemaT]] = Omitted()

    # array
    prefix_items: Omittable[Sequence[JSONSchemaT]] = Omitted()
    items: Omittable[JSONSchemaT] = Omitted()
    contains: Omittable[JSONSchemaT] = Omitted()

    # object
    properties: Omittable[JSONObject[JSONSchemaT]] = Omitted()
    pattern_properties: Omittable[JSONObject[JSONSchemaT]] = Omitted()
    additional_properties: Omittable[JSONSchemaT] = Omitted()
    property_names: Omittable[JSONSchemaT] = Omitted()

    # Unevaluated Locations
    unevaluated_items: Omittable[JSONObject[JSONSchemaT]] = Omitted()
    unevaluated_properties: Omittable[JSONObject[JSONSchemaT]] = Omitted()


@dataclass
class _JSONSchemaValidation(Generic[JSONSchemaT]):
    # common
    type: Omittable[Union[JSONSchemaType, Sequence[JSONSchemaType]]] = Omitted()
    enum: Omittable[Sequence[JSONValue]] = Omitted()
    const: Omittable[JSONValue] = Omitted()

    format: Omittable[Union[JSONSchemaBuiltinFormat, str]] = Omitted()

    # numeric
    multiple_of: Omittable[JSONNumeric] = Omitted()
    maximum: Omittable[JSONNumeric] = Omitted()
    exclusive_maximum: Omittable[JSONNumeric] = Omitted()
    minimum: Omittable[JSONNumeric] = Omitted()
    exclusive_minimum: Omittable[JSONNumeric] = Omitted()

    # string
    max_length: Omittable[int] = Omitted()
    min_length: Omittable[int] = Omitted()
    pattern: Omittable[str] = Omitted()

    content_encoding: Omittable[str] = Omitted()
    content_media_type: Omittable[str] = Omitted()
    content_schema: Omittable[JSONSchemaT] = Omitted()

    # array
    max_items: Omittable[int] = Omitted()
    min_items: Omittable[int] = Omitted()
    unique_items: Omittable[bool] = Omitted()
    max_contains: Omittable[int] = Omitted()
    min_contains: Omittable[int] = Omitted()

    # object
    max_properties: Omittable[int] = Omitted()
    min_properties: Omittable[int] = Omitted()
    required: Omittable[Sequence[str]] = Omitted()
    dependent_required: Omittable[JSONObject[Sequence[str]]] = Omitted()


@dataclass
class _JSONSchemaAnnotations:
    title: Omittable[str] = Omitted()
    description: Omittable[str] = Omitted()
    default: Omittable[JSONValue] = Omitted()
    deprecated: Omittable[bool] = Omitted()
    read_only: Omittable[bool] = Omitted()
    write_only: Omittable[bool] = Omitted()
    examples: Omittable[Sequence[JSONValue]] = Omitted()


@dataclass
class BaseJSONSchema(
    _JSONSchemaCore[RefT, JSONSchemaT],
    _JSONSchemaSubschemas[JSONSchemaT],
    _JSONSchemaValidation[JSONSchemaT],
    _JSONSchemaAnnotations,
    Generic[RefT, JSONSchemaT],
):
    extra_keywords: JSONObject[JSONValue] = field(default_factory=dict)

    def __repr__(self):
        body = ", ".join(
            f"{key}={value!r}"
            for key, value in vars(self).items()
            if value != ({} if key == "extra_keywords" else Omitted())
        )
        return f"{type(self).__name__}({body})"


class JSONSchemaDialect(str, Enum):
    DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"
