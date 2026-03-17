from __future__ import annotations

import enum as _enum
from collections import defaultdict
from contextlib import contextmanager
from functools import cached_property, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Union
from urllib.parse import ParseResult, unquote
from warnings import warn

from pydantic import (
    Field,
)

from datamodel_code_generator import (
    InvalidClassNameError,
    load_yaml,
    load_yaml_from_path,
    snooper_to_methods,
)
from datamodel_code_generator.format import DEFAULT_FORMATTERS, Formatter, PythonVersion, PythonVersionMin
from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model import pydantic as pydantic_model
from datamodel_code_generator.model.base import UNDEFINED, get_module_name
from datamodel_code_generator.model.dataclass import DataClass
from datamodel_code_generator.model.enum import Enum
from datamodel_code_generator.parser import DefaultPutDict, LiteralType
from datamodel_code_generator.parser.base import (
    SPECIAL_PATH_FORMAT,
    Parser,
    Source,
    escape_characters,
    get_special_path,
    title_to_class_name,
)
from datamodel_code_generator.reference import ModelType, Reference, is_url
from datamodel_code_generator.types import (
    DataType,
    DataTypeManager,
    EmptyDataType,
    StrictTypes,
    Types,
    UnionIntFloat,
)
from datamodel_code_generator.util import (
    PYDANTIC_V2,
    BaseModel,
    field_validator,
    model_validator,
)

if PYDANTIC_V2:
    from pydantic import ConfigDict

from datamodel_code_generator.format import DatetimeClassType

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence


def unescape_json_pointer_segment(segment: str) -> str:
    # Unescape ~1, ~0, and percent-encoding
    return unquote(segment.replace("~1", "/").replace("~0", "~"))


def get_model_by_path(schema: dict[str, Any] | list[Any], keys: list[str] | list[int]) -> dict[Any, Any]:
    model: dict[Any, Any] | list[Any]
    if not keys:
        model = schema
    else:
        # Unescape the key if it's a string (JSON pointer segment)
        key = keys[0]
        if isinstance(key, str):
            key = unescape_json_pointer_segment(key)
        if len(keys) == 1:
            model = schema.get(str(key), {}) if isinstance(schema, dict) else schema[int(key)]
        elif isinstance(schema, dict):
            model = get_model_by_path(schema[str(key)], keys[1:])
        else:
            model = get_model_by_path(schema[int(key)], keys[1:])
    if isinstance(model, dict):
        return model
    msg = f"Does not support json pointer to array. schema={schema}, key={keys}"
    raise NotImplementedError(  # pragma: no cover
        msg
    )


json_schema_data_formats: dict[str, dict[str, Types]] = {
    "integer": {
        "int32": Types.int32,
        "int64": Types.int64,
        "default": Types.integer,
        "date-time": Types.date_time,
        "unix-time": Types.int64,
    },
    "number": {
        "float": Types.float,
        "double": Types.double,
        "decimal": Types.decimal,
        "date-time": Types.date_time,
        "time": Types.time,
        "default": Types.number,
    },
    "string": {
        "default": Types.string,
        "byte": Types.byte,  # base64 encoded string
        "binary": Types.binary,
        "date": Types.date,
        "date-time": Types.date_time,
        "duration": Types.timedelta,
        "time": Types.time,
        "password": Types.password,
        "path": Types.path,
        "email": Types.email,
        "idn-email": Types.email,
        "uuid": Types.uuid,
        "uuid1": Types.uuid1,
        "uuid2": Types.uuid2,
        "uuid3": Types.uuid3,
        "uuid4": Types.uuid4,
        "uuid5": Types.uuid5,
        "uri": Types.uri,
        "uri-reference": Types.string,
        "hostname": Types.hostname,
        "ipv4": Types.ipv4,
        "ipv4-network": Types.ipv4_network,
        "ipv6": Types.ipv6,
        "ipv6-network": Types.ipv6_network,
        "decimal": Types.decimal,
        "integer": Types.integer,
    },
    "boolean": {"default": Types.boolean},
    "object": {"default": Types.object},
    "null": {"default": Types.null},
    "array": {"default": Types.array},
}


class JSONReference(_enum.Enum):
    LOCAL = "LOCAL"
    REMOTE = "REMOTE"
    URL = "URL"


class Discriminator(BaseModel):
    propertyName: str  # noqa: N815
    mapping: Optional[dict[str, str]] = None  # noqa: UP045


class JsonSchemaObject(BaseModel):
    if not TYPE_CHECKING:
        if PYDANTIC_V2:

            @classmethod
            def get_fields(cls) -> dict[str, Any]:
                return cls.model_fields

        else:

            @classmethod
            def get_fields(cls) -> dict[str, Any]:
                return cls.__fields__

            @classmethod
            def model_rebuild(cls) -> None:
                cls.update_forward_refs()

    __constraint_fields__: set[str] = {  # noqa: RUF012
        "exclusiveMinimum",
        "minimum",
        "exclusiveMaximum",
        "maximum",
        "multipleOf",
        "minItems",
        "maxItems",
        "minLength",
        "maxLength",
        "pattern",
        "uniqueItems",
    }
    __extra_key__: str = SPECIAL_PATH_FORMAT.format("extras")

    @model_validator(mode="before")
    def validate_exclusive_maximum_and_exclusive_minimum(cls, values: Any) -> Any:  # noqa: N805
        if not isinstance(values, dict):
            return values
        exclusive_maximum: float | bool | None = values.get("exclusiveMaximum")
        exclusive_minimum: float | bool | None = values.get("exclusiveMinimum")

        if exclusive_maximum is True:
            values["exclusiveMaximum"] = values["maximum"]
            del values["maximum"]
        elif exclusive_maximum is False:
            del values["exclusiveMaximum"]
        if exclusive_minimum is True:
            values["exclusiveMinimum"] = values["minimum"]
            del values["minimum"]
        elif exclusive_minimum is False:
            del values["exclusiveMinimum"]
        return values

    @field_validator("ref")
    def validate_ref(cls, value: Any) -> Any:  # noqa: N805
        if isinstance(value, str) and "#" in value:
            if value.endswith("#/"):
                return value[:-1]
            if "#/" in value or value[0] == "#" or value[-1] == "#":
                return value
            return value.replace("#", "#/")
        return value

    @field_validator("required", mode="before")
    def validate_required(cls, value: Any) -> Any:  # noqa: N805
        if value is None:
            return []
        if isinstance(value, list):  # noqa: PLR1702
            # Filter to only include valid strings, excluding invalid objects
            required_fields: list[str] = []
            for item in value:
                if isinstance(item, str):
                    required_fields.append(item)

                # In some cases, the required field can include "anyOf", "oneOf", or "allOf" as a dict (#2297)
                elif isinstance(item, dict):
                    for key, val in item.items():
                        if isinstance(val, list):
                            # If 'anyOf' or "oneOf" is present, we won't include it in required fields
                            if key in {"anyOf", "oneOf"}:
                                continue

                            if key == "allOf":
                                # If 'allOf' is present, we include them as required fields
                                required_fields.extend(sub_item for sub_item in val if isinstance(sub_item, str))

            value = required_fields

        return value

    items: Optional[Union[list[JsonSchemaObject], JsonSchemaObject, bool]] = None  # noqa: UP007, UP045
    uniqueItems: Optional[bool] = None  # noqa: N815, UP045
    type: Optional[Union[str, list[str]]] = None  # noqa: UP007, UP045
    format: Optional[str] = None  # noqa: UP045
    pattern: Optional[str] = None  # noqa: UP045
    minLength: Optional[int] = None  # noqa:  N815,UP045
    maxLength: Optional[int] = None  # noqa:  N815,UP045
    minimum: Optional[UnionIntFloat] = None  # noqa:  UP045
    maximum: Optional[UnionIntFloat] = None  # noqa:  UP045
    minItems: Optional[int] = None  # noqa:  N815,UP045
    maxItems: Optional[int] = None  # noqa:  N815,UP045
    multipleOf: Optional[float] = None  # noqa: N815, UP045
    exclusiveMaximum: Optional[Union[float, bool]] = None  # noqa: N815, UP007, UP045
    exclusiveMinimum: Optional[Union[float, bool]] = None  # noqa: N815, UP007, UP045
    additionalProperties: Optional[Union[JsonSchemaObject, bool]] = None  # noqa: N815, UP007, UP045
    patternProperties: Optional[dict[str, JsonSchemaObject]] = None  # noqa: N815, UP045
    oneOf: list[JsonSchemaObject] = []  # noqa: N815, RUF012
    anyOf: list[JsonSchemaObject] = []  # noqa: N815, RUF012
    allOf: list[JsonSchemaObject] = []  # noqa: N815, RUF012
    enum: list[Any] = []  # noqa: RUF012
    writeOnly: Optional[bool] = None  # noqa: N815, UP045
    readOnly: Optional[bool] = None  # noqa: N815, UP045
    properties: Optional[dict[str, Union[JsonSchemaObject, bool]]] = None  # noqa: UP007, UP045
    required: list[str] = []  # noqa: RUF012
    ref: Optional[str] = Field(default=None, alias="$ref")  # noqa: UP045
    nullable: Optional[bool] = False  # noqa: UP045
    x_enum_varnames: list[str] = Field(default=[], alias="x-enum-varnames")
    description: Optional[str] = None  # noqa: UP045
    title: Optional[str] = None  # noqa: UP045
    example: Any = None
    examples: Any = None
    default: Any = None
    id: Optional[str] = Field(default=None, alias="$id")  # noqa: UP045
    custom_type_path: Optional[str] = Field(default=None, alias="customTypePath")  # noqa: UP045
    custom_base_path: Optional[str] = Field(default=None, alias="customBasePath")  # noqa: UP045
    extras: dict[str, Any] = Field(alias=__extra_key__, default_factory=dict)
    discriminator: Optional[Union[Discriminator, str]] = None  # noqa: UP007, UP045
    if PYDANTIC_V2:
        model_config = ConfigDict(  # pyright: ignore[reportPossiblyUnboundVariable]
            arbitrary_types_allowed=True,
            ignored_types=(cached_property,),
        )
    else:

        class Config:
            arbitrary_types_allowed = True
            keep_untouched = (cached_property,)
            smart_casts = True

    if not TYPE_CHECKING:

        def __init__(self, **data: Any) -> None:
            super().__init__(**data)
            self.extras = {k: v for k, v in data.items() if k not in EXCLUDE_FIELD_KEYS}
            if "const" in data.get(self.__extra_key__, {}):
                self.extras["const"] = data[self.__extra_key__]["const"]

    @cached_property
    def is_object(self) -> bool:
        return self.properties is not None or (
            self.type == "object" and not self.allOf and not self.oneOf and not self.anyOf and not self.ref
        )

    @cached_property
    def is_array(self) -> bool:
        return self.items is not None or self.type == "array"

    @cached_property
    def ref_object_name(self) -> str:  # pragma: no cover
        return (self.ref or "").rsplit("/", 1)[-1]

    @field_validator("items", mode="before")
    def validate_items(cls, values: Any) -> Any:  # noqa: N805
        # this condition expects empty dict
        return values or None

    @cached_property
    def has_default(self) -> bool:
        return "default" in self.__fields_set__ or "default_factory" in self.extras

    @cached_property
    def has_constraint(self) -> bool:
        return bool(self.__constraint_fields__ & self.__fields_set__)

    @cached_property
    def ref_type(self) -> JSONReference | None:
        if self.ref:
            return get_ref_type(self.ref)
        return None  # pragma: no cover

    @cached_property
    def type_has_null(self) -> bool:
        return isinstance(self.type, list) and "null" in self.type


@lru_cache
def get_ref_type(ref: str) -> JSONReference:
    if ref[0] == "#":
        return JSONReference.LOCAL
    if is_url(ref):
        return JSONReference.URL
    return JSONReference.REMOTE


def _get_type(type_: str, format__: str | None = None) -> Types:
    if type_ not in json_schema_data_formats:
        return Types.any
    data_formats: Types | None = json_schema_data_formats[type_].get("default" if format__ is None else format__)
    if data_formats is not None:
        return data_formats

    warn(f"format of {format__!r} not understood for {type_!r} - using default", stacklevel=2)
    return json_schema_data_formats[type_]["default"]


JsonSchemaObject.model_rebuild()

DEFAULT_FIELD_KEYS: set[str] = {
    "example",
    "examples",
    "description",
    "discriminator",
    "title",
    "const",
    "default_factory",
}

EXCLUDE_FIELD_KEYS_IN_JSON_SCHEMA: set[str] = {
    "readOnly",
    "writeOnly",
}

EXCLUDE_FIELD_KEYS = (
    set(JsonSchemaObject.get_fields())  # pyright: ignore[reportAttributeAccessIssue]
    - DEFAULT_FIELD_KEYS
    - EXCLUDE_FIELD_KEYS_IN_JSON_SCHEMA
) | {
    "$id",
    "$ref",
    JsonSchemaObject.__extra_key__,
}


@snooper_to_methods()  # noqa: PLR0904
class JsonSchemaParser(Parser):
    SCHEMA_PATHS: ClassVar[list[str]] = ["#/definitions", "#/$defs"]
    SCHEMA_OBJECT_TYPE: ClassVar[type[JsonSchemaObject]] = JsonSchemaObject

    def __init__(  # noqa: PLR0913
        self,
        source: str | Path | list[Path] | ParseResult,
        *,
        data_model_type: type[DataModel] = pydantic_model.BaseModel,
        data_model_root_type: type[DataModel] = pydantic_model.CustomRootType,
        data_type_manager_type: type[DataTypeManager] = pydantic_model.DataTypeManager,
        data_model_field_type: type[DataModelFieldBase] = pydantic_model.DataModelField,
        base_class: str | None = None,
        additional_imports: list[str] | None = None,
        custom_template_dir: Path | None = None,
        extra_template_data: defaultdict[str, dict[str, Any]] | None = None,
        target_python_version: PythonVersion = PythonVersionMin,
        dump_resolve_reference_action: Callable[[Iterable[str]], str] | None = None,
        validation: bool = False,
        field_constraints: bool = False,
        snake_case_field: bool = False,
        strip_default_none: bool = False,
        aliases: Mapping[str, str] | None = None,
        allow_population_by_field_name: bool = False,
        apply_default_values_for_required_fields: bool = False,
        allow_extra_fields: bool = False,
        extra_fields: str | None = None,
        force_optional_for_required_fields: bool = False,
        class_name: str | None = None,
        use_standard_collections: bool = False,
        base_path: Path | None = None,
        use_schema_description: bool = False,
        use_field_description: bool = False,
        use_default_kwarg: bool = False,
        reuse_model: bool = False,
        encoding: str = "utf-8",
        enum_field_as_literal: LiteralType | None = None,
        use_one_literal_as_default: bool = False,
        set_default_enum_member: bool = False,
        use_subclass_enum: bool = False,
        strict_nullable: bool = False,
        use_generic_container_types: bool = False,
        enable_faux_immutability: bool = False,
        remote_text_cache: DefaultPutDict[str, str] | None = None,
        disable_appending_item_suffix: bool = False,
        strict_types: Sequence[StrictTypes] | None = None,
        empty_enum_field_name: str | None = None,
        custom_class_name_generator: Callable[[str], str] | None = None,
        field_extra_keys: set[str] | None = None,
        field_include_all_keys: bool = False,
        field_extra_keys_without_x_prefix: set[str] | None = None,
        wrap_string_literal: bool | None = None,
        use_title_as_name: bool = False,
        use_operation_id_as_name: bool = False,
        use_unique_items_as_set: bool = False,
        http_headers: Sequence[tuple[str, str]] | None = None,
        http_ignore_tls: bool = False,
        use_annotated: bool = False,
        use_non_positive_negative_number_constrained_types: bool = False,
        original_field_name_delimiter: str | None = None,
        use_double_quotes: bool = False,
        use_union_operator: bool = False,
        allow_responses_without_content: bool = False,
        collapse_root_models: bool = False,
        special_field_name_prefix: str | None = None,
        remove_special_field_name_prefix: bool = False,
        capitalise_enum_members: bool = False,
        keep_model_order: bool = False,
        known_third_party: list[str] | None = None,
        custom_formatters: list[str] | None = None,
        custom_formatters_kwargs: dict[str, Any] | None = None,
        use_pendulum: bool = False,
        http_query_parameters: Sequence[tuple[str, str]] | None = None,
        treat_dot_as_module: bool = False,
        use_exact_imports: bool = False,
        default_field_extras: dict[str, Any] | None = None,
        target_datetime_class: DatetimeClassType | None = None,
        keyword_only: bool = False,
        frozen_dataclasses: bool = False,
        no_alias: bool = False,
        formatters: list[Formatter] = DEFAULT_FORMATTERS,
        parent_scoped_naming: bool = False,
    ) -> None:
        target_datetime_class = target_datetime_class or DatetimeClassType.Awaredatetime
        super().__init__(
            source=source,
            data_model_type=data_model_type,
            data_model_root_type=data_model_root_type,
            data_type_manager_type=data_type_manager_type,
            data_model_field_type=data_model_field_type,
            base_class=base_class,
            additional_imports=additional_imports,
            custom_template_dir=custom_template_dir,
            extra_template_data=extra_template_data,
            target_python_version=target_python_version,
            dump_resolve_reference_action=dump_resolve_reference_action,
            validation=validation,
            field_constraints=field_constraints,
            snake_case_field=snake_case_field,
            strip_default_none=strip_default_none,
            aliases=aliases,
            allow_population_by_field_name=allow_population_by_field_name,
            allow_extra_fields=allow_extra_fields,
            extra_fields=extra_fields,
            apply_default_values_for_required_fields=apply_default_values_for_required_fields,
            force_optional_for_required_fields=force_optional_for_required_fields,
            class_name=class_name,
            use_standard_collections=use_standard_collections,
            base_path=base_path,
            use_schema_description=use_schema_description,
            use_field_description=use_field_description,
            use_default_kwarg=use_default_kwarg,
            reuse_model=reuse_model,
            encoding=encoding,
            enum_field_as_literal=enum_field_as_literal,
            use_one_literal_as_default=use_one_literal_as_default,
            set_default_enum_member=set_default_enum_member,
            use_subclass_enum=use_subclass_enum,
            strict_nullable=strict_nullable,
            use_generic_container_types=use_generic_container_types,
            enable_faux_immutability=enable_faux_immutability,
            remote_text_cache=remote_text_cache,
            disable_appending_item_suffix=disable_appending_item_suffix,
            strict_types=strict_types,
            empty_enum_field_name=empty_enum_field_name,
            custom_class_name_generator=custom_class_name_generator,
            field_extra_keys=field_extra_keys,
            field_include_all_keys=field_include_all_keys,
            field_extra_keys_without_x_prefix=field_extra_keys_without_x_prefix,
            wrap_string_literal=wrap_string_literal,
            use_title_as_name=use_title_as_name,
            use_operation_id_as_name=use_operation_id_as_name,
            use_unique_items_as_set=use_unique_items_as_set,
            http_headers=http_headers,
            http_ignore_tls=http_ignore_tls,
            use_annotated=use_annotated,
            use_non_positive_negative_number_constrained_types=use_non_positive_negative_number_constrained_types,
            original_field_name_delimiter=original_field_name_delimiter,
            use_double_quotes=use_double_quotes,
            use_union_operator=use_union_operator,
            allow_responses_without_content=allow_responses_without_content,
            collapse_root_models=collapse_root_models,
            special_field_name_prefix=special_field_name_prefix,
            remove_special_field_name_prefix=remove_special_field_name_prefix,
            capitalise_enum_members=capitalise_enum_members,
            keep_model_order=keep_model_order,
            known_third_party=known_third_party,
            custom_formatters=custom_formatters,
            custom_formatters_kwargs=custom_formatters_kwargs,
            use_pendulum=use_pendulum,
            http_query_parameters=http_query_parameters,
            treat_dot_as_module=treat_dot_as_module,
            use_exact_imports=use_exact_imports,
            default_field_extras=default_field_extras,
            target_datetime_class=target_datetime_class,
            keyword_only=keyword_only,
            frozen_dataclasses=frozen_dataclasses,
            no_alias=no_alias,
            formatters=formatters,
            parent_scoped_naming=parent_scoped_naming,
        )

        self.remote_object_cache: DefaultPutDict[str, dict[str, Any]] = DefaultPutDict()
        self.raw_obj: dict[Any, Any] = {}
        self._root_id: Optional[str] = None  # noqa: UP045
        self._root_id_base_path: Optional[str] = None  # noqa: UP045
        self.reserved_refs: defaultdict[tuple[str, ...], set[str]] = defaultdict(set)
        self.field_keys: set[str] = {
            *DEFAULT_FIELD_KEYS,
            *self.field_extra_keys,
            *self.field_extra_keys_without_x_prefix,
        }

        if self.data_model_field_type.can_have_extra_keys:
            self.get_field_extra_key: Callable[[str], str] = (
                lambda key: self.model_resolver.get_valid_field_name_and_alias(key)[0]
            )

        else:
            self.get_field_extra_key = lambda key: key

    def get_field_extras(self, obj: JsonSchemaObject) -> dict[str, Any]:
        if self.field_include_all_keys:
            extras = {
                self.get_field_extra_key(k.lstrip("x-") if k in self.field_extra_keys_without_x_prefix else k): v
                for k, v in obj.extras.items()
            }
        else:
            extras = {
                self.get_field_extra_key(k.lstrip("x-") if k in self.field_extra_keys_without_x_prefix else k): v
                for k, v in obj.extras.items()
                if k in self.field_keys
            }
        if self.default_field_extras:
            extras.update(self.default_field_extras)
        return extras

    @cached_property
    def schema_paths(self) -> list[tuple[str, list[str]]]:
        return [(s, s.lstrip("#/").split("/")) for s in self.SCHEMA_PATHS]

    @property
    def root_id(self) -> str | None:
        return self.model_resolver.root_id

    @root_id.setter
    def root_id(self, value: str | None) -> None:
        self.model_resolver.set_root_id(value)

    def should_parse_enum_as_literal(self, obj: JsonSchemaObject) -> bool:
        return self.enum_field_as_literal == LiteralType.All or (
            self.enum_field_as_literal == LiteralType.One and len(obj.enum) == 1
        )

    def is_constraints_field(self, obj: JsonSchemaObject) -> bool:
        return obj.is_array or (
            self.field_constraints and not (obj.ref or obj.anyOf or obj.oneOf or obj.allOf or obj.is_object or obj.enum)
        )

    def get_object_field(  # noqa: PLR0913
        self,
        *,
        field_name: str | None,
        field: JsonSchemaObject,
        required: bool,
        field_type: DataType,
        alias: str | None,
        original_field_name: str | None,
    ) -> DataModelFieldBase:
        return self.data_model_field_type(
            name=field_name,
            default=field.default,
            data_type=field_type,
            required=required,
            alias=alias,
            constraints=field.dict() if self.is_constraints_field(field) else None,
            nullable=field.nullable if self.strict_nullable and (field.has_default or required) else None,
            strip_default_none=self.strip_default_none,
            extras=self.get_field_extras(field),
            use_annotated=self.use_annotated,
            use_field_description=self.use_field_description,
            use_default_kwarg=self.use_default_kwarg,
            original_name=original_field_name,
            has_default=field.has_default,
            type_has_null=field.type_has_null,
        )

    def get_data_type(self, obj: JsonSchemaObject) -> DataType:
        if obj.type is None:
            if "const" in obj.extras:
                return self.data_type_manager.get_data_type_from_value(obj.extras["const"])
            return self.data_type_manager.get_data_type(
                Types.any,
            )

        def _get_data_type(type_: str, format__: str) -> DataType:
            return self.data_type_manager.get_data_type(
                _get_type(type_, format__),
                **obj.dict() if not self.field_constraints else {},
            )

        if isinstance(obj.type, list):
            return self.data_type(
                data_types=[_get_data_type(t, obj.format or "default") for t in obj.type if t != "null"],
                is_optional="null" in obj.type,
            )
        return _get_data_type(obj.type, obj.format or "default")

    def get_ref_data_type(self, ref: str) -> DataType:
        reference = self.model_resolver.add_ref(ref)
        return self.data_type(reference=reference)

    def set_additional_properties(self, path: str, obj: JsonSchemaObject) -> None:
        if isinstance(obj.additionalProperties, bool):
            self.extra_template_data[path]["additionalProperties"] = obj.additionalProperties

    def set_title(self, path: str, obj: JsonSchemaObject) -> None:
        if obj.title:
            self.extra_template_data[path]["title"] = obj.title

    def _deep_merge(self, dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
        result = dict1.copy()
        for key, value in dict2.items():
            if key in result:
                if isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._deep_merge(result[key], value)
                    continue
                if isinstance(result[key], list) and isinstance(value, list):
                    result[key] = result[key] + value  # noqa: PLR6104
                    continue
            result[key] = value
        return result

    def parse_combined_schema(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        target_attribute_name: str,
    ) -> list[DataType]:
        base_object = obj.dict(exclude={target_attribute_name}, exclude_unset=True, by_alias=True)
        combined_schemas: list[JsonSchemaObject] = []
        refs = []
        for index, target_attribute in enumerate(getattr(obj, target_attribute_name, [])):
            if target_attribute.ref:
                combined_schemas.append(target_attribute)
                refs.append(index)
                # TODO: support partial ref
            else:
                combined_schemas.append(
                    self.SCHEMA_OBJECT_TYPE.parse_obj(
                        self._deep_merge(
                            base_object,
                            target_attribute.dict(exclude_unset=True, by_alias=True),
                        )
                    )
                )

        parsed_schemas = self.parse_list_item(
            name,
            combined_schemas,
            path,
            obj,
            singular_name=False,
        )
        common_path_keyword = f"{target_attribute_name}Common"
        return [
            self._parse_object_common_part(
                name,
                obj,
                [*get_special_path(common_path_keyword, path), str(i)],
                ignore_duplicate_model=True,
                fields=[],
                base_classes=[d.reference],
                required=[],
            )
            if i in refs and d.reference
            else d
            for i, d in enumerate(parsed_schemas)
        ]

    def parse_any_of(self, name: str, obj: JsonSchemaObject, path: list[str]) -> list[DataType]:
        return self.parse_combined_schema(name, obj, path, "anyOf")

    def parse_one_of(self, name: str, obj: JsonSchemaObject, path: list[str]) -> list[DataType]:
        return self.parse_combined_schema(name, obj, path, "oneOf")

    def _create_data_model(self, model_type: type[DataModel] | None = None, **kwargs: Any) -> DataModel:
        """Create data model instance with conditional frozen parameter for DataClass."""
        data_model_class = model_type or self.data_model_type
        if issubclass(data_model_class, DataClass):
            kwargs["frozen"] = self.frozen_dataclasses
        return data_model_class(**kwargs)

    def _parse_object_common_part(  # noqa: PLR0913, PLR0917
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        ignore_duplicate_model: bool,  # noqa: FBT001
        fields: list[DataModelFieldBase],
        base_classes: list[Reference],
        required: list[str],
    ) -> DataType:
        if obj.properties:
            fields.extend(
                self.parse_object_fields(
                    obj, path, get_module_name(name, None, treat_dot_as_module=self.treat_dot_as_module)
                )
            )
        # ignore an undetected object
        if ignore_duplicate_model and not fields and len(base_classes) == 1:
            with self.model_resolver.current_base_path_context(self.model_resolver._base_path):  # noqa: SLF001
                self.model_resolver.delete(path)
                return self.data_type(reference=base_classes[0])
        if required:
            for field in fields:
                if self.force_optional_for_required_fields or (  # pragma: no cover
                    self.apply_default_values_for_required_fields and field.has_default
                ):
                    continue  # pragma: no cover
                if (field.original_name or field.name) in required:
                    field.required = True
        if obj.required:
            field_name_to_field = {f.original_name or f.name: f for f in fields}
            for required_ in obj.required:
                if required_ in field_name_to_field:
                    field = field_name_to_field[required_]
                    if self.force_optional_for_required_fields or (
                        self.apply_default_values_for_required_fields and field.has_default
                    ):
                        continue
                    field.required = True
                else:
                    fields.append(
                        self.data_model_field_type(required=True, original_name=required_, data_type=DataType())
                    )
        if self.use_title_as_name and obj.title:  # pragma: no cover
            name = obj.title
        reference = self.model_resolver.add(path, name, class_name=True, loaded=True)
        self.set_additional_properties(reference.path, obj)

        data_model_type = self._create_data_model(
            reference=reference,
            fields=fields,
            base_classes=base_classes,
            custom_base_class=obj.custom_base_path or self.base_class,
            custom_template_dir=self.custom_template_dir,
            extra_template_data=self.extra_template_data,
            path=self.current_source_path,
            description=obj.description if self.use_schema_description else None,
            keyword_only=self.keyword_only,
            treat_dot_as_module=self.treat_dot_as_module,
        )
        self.results.append(data_model_type)

        return self.data_type(reference=reference)

    def _parse_all_of_item(  # noqa: PLR0913, PLR0917
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        fields: list[DataModelFieldBase],
        base_classes: list[Reference],
        required: list[str],
        union_models: list[Reference],
    ) -> None:
        for all_of_item in obj.allOf:
            if all_of_item.ref:  # $ref
                base_classes.append(self.model_resolver.add_ref(all_of_item.ref))
            else:
                module_name = get_module_name(name, None, treat_dot_as_module=self.treat_dot_as_module)
                object_fields = self.parse_object_fields(
                    all_of_item,
                    path,
                    module_name,
                )

                if object_fields:
                    fields.extend(object_fields)
                elif all_of_item.required:
                    required.extend(all_of_item.required)
                self._parse_all_of_item(
                    name,
                    all_of_item,
                    path,
                    fields,
                    base_classes,
                    required,
                    union_models,
                )
                if all_of_item.anyOf:
                    self.model_resolver.add(path, name, class_name=True, loaded=True)
                    union_models.extend(d.reference for d in self.parse_any_of(name, all_of_item, path) if d.reference)
                if all_of_item.oneOf:
                    self.model_resolver.add(path, name, class_name=True, loaded=True)
                    union_models.extend(d.reference for d in self.parse_one_of(name, all_of_item, path) if d.reference)

    def parse_all_of(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        ignore_duplicate_model: bool = False,  # noqa: FBT001, FBT002
    ) -> DataType:
        if len(obj.allOf) == 1 and not obj.properties:
            single_obj = obj.allOf[0]
            if (
                single_obj.ref
                and single_obj.ref_type == JSONReference.LOCAL
                and get_model_by_path(self.raw_obj, single_obj.ref[2:].split("/")).get("enum")
            ):
                return self.get_ref_data_type(single_obj.ref)
        fields: list[DataModelFieldBase] = []
        base_classes: list[Reference] = []
        required: list[str] = []
        union_models: list[Reference] = []
        self._parse_all_of_item(name, obj, path, fields, base_classes, required, union_models)
        if not union_models:
            return self._parse_object_common_part(
                name, obj, path, ignore_duplicate_model, fields, base_classes, required
            )
        reference = self.model_resolver.add(path, name, class_name=True, loaded=True)
        all_of_data_type = self._parse_object_common_part(
            name,
            obj,
            get_special_path("allOf", path),
            ignore_duplicate_model,
            fields,
            base_classes,
            required,
        )
        assert all_of_data_type.reference is not None
        data_type = self.data_type(
            data_types=[
                self._parse_object_common_part(
                    name,
                    obj,
                    get_special_path(f"union_model-{index}", path),
                    ignore_duplicate_model,
                    [],
                    [union_model, all_of_data_type.reference],
                    [],
                )
                for index, union_model in enumerate(union_models)
            ]
        )
        field = self.get_object_field(
            field_name=None,
            field=obj,
            required=True,
            field_type=data_type,
            alias=None,
            original_field_name=None,
        )
        data_model_root = self.data_model_root_type(
            reference=reference,
            fields=[field],
            custom_base_class=obj.custom_base_path or self.base_class,
            custom_template_dir=self.custom_template_dir,
            extra_template_data=self.extra_template_data,
            path=self.current_source_path,
            description=obj.description if self.use_schema_description else None,
            nullable=obj.type_has_null,
            treat_dot_as_module=self.treat_dot_as_module,
        )
        self.results.append(data_model_root)
        return self.data_type(reference=reference)

    def parse_object_fields(
        self,
        obj: JsonSchemaObject,
        path: list[str],
        module_name: Optional[str] = None,  # noqa: UP045
    ) -> list[DataModelFieldBase]:
        properties: dict[str, JsonSchemaObject | bool] = {} if obj.properties is None else obj.properties
        requires: set[str] = {*()} if obj.required is None else {*obj.required}
        fields: list[DataModelFieldBase] = []

        exclude_field_names: set[str] = set()
        for original_field_name, field in properties.items():
            field_name, alias = self.model_resolver.get_valid_field_name_and_alias(
                original_field_name, excludes=exclude_field_names
            )
            modular_name = f"{module_name}.{field_name}" if module_name else field_name

            exclude_field_names.add(field_name)

            if isinstance(field, bool):
                fields.append(
                    self.data_model_field_type(
                        name=field_name,
                        data_type=self.data_type_manager.get_data_type(
                            Types.any,
                        ),
                        required=False if self.force_optional_for_required_fields else original_field_name in requires,
                        alias=alias,
                        strip_default_none=self.strip_default_none,
                        use_annotated=self.use_annotated,
                        use_field_description=self.use_field_description,
                        original_name=original_field_name,
                    )
                )
                continue

            field_type = self.parse_item(modular_name, field, [*path, field_name])

            if self.force_optional_for_required_fields or (
                self.apply_default_values_for_required_fields and field.has_default
            ):
                required: bool = False
            else:
                required = original_field_name in requires
            fields.append(
                self.get_object_field(
                    field_name=field_name,
                    field=field,
                    required=required,
                    field_type=field_type,
                    alias=alias,
                    original_field_name=original_field_name,
                )
            )
        return fields

    def parse_object(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        singular_name: bool = False,  # noqa: FBT001, FBT002
        unique: bool = True,  # noqa: FBT001, FBT002
    ) -> DataType:
        if not unique:  # pragma: no cover
            warn(
                f"{self.__class__.__name__}.parse_object() ignore `unique` argument."
                f"An object name must be unique."
                f"This argument will be removed in a future version",
                stacklevel=2,
            )
        if self.use_title_as_name and obj.title:
            name = obj.title
        reference = self.model_resolver.add(
            path,
            name,
            class_name=True,
            singular_name=singular_name,
            loaded=True,
        )
        class_name = reference.name
        self.set_title(reference.path, obj)
        fields = self.parse_object_fields(
            obj, path, get_module_name(class_name, None, treat_dot_as_module=self.treat_dot_as_module)
        )
        if fields or not isinstance(obj.additionalProperties, JsonSchemaObject):
            data_model_type_class = self.data_model_type
        else:
            fields.append(
                self.get_object_field(
                    field_name=None,
                    field=obj.additionalProperties,
                    required=True,
                    original_field_name=None,
                    field_type=self.data_type(
                        data_types=[
                            self.parse_item(
                                # TODO: Improve naming for nested ClassName
                                name,
                                obj.additionalProperties,
                                [*path, "additionalProperties"],
                            )
                        ],
                        is_dict=True,
                    ),
                    alias=None,
                )
            )
            data_model_type_class = self.data_model_root_type

        self.set_additional_properties(reference.path, obj)

        data_model_type = self._create_data_model(
            model_type=data_model_type_class,
            reference=reference,
            fields=fields,
            custom_base_class=obj.custom_base_path or self.base_class,
            custom_template_dir=self.custom_template_dir,
            extra_template_data=self.extra_template_data,
            path=self.current_source_path,
            description=obj.description if self.use_schema_description else None,
            nullable=obj.type_has_null,
            keyword_only=self.keyword_only,
            treat_dot_as_module=self.treat_dot_as_module,
        )
        self.results.append(data_model_type)
        return self.data_type(reference=reference)

    def parse_pattern_properties(
        self,
        name: str,
        pattern_properties: dict[str, JsonSchemaObject],
        path: list[str],
    ) -> DataType:
        return self.data_type(
            data_types=[
                self.data_type(
                    data_types=[
                        self.parse_item(
                            name,
                            kv[1],
                            get_special_path(f"patternProperties/{i}", path),
                        )
                    ],
                    is_dict=True,
                    dict_key=self.data_type_manager.get_data_type(
                        Types.string,
                        pattern=kv[0] if not self.field_constraints else None,
                    ),
                )
                for i, kv in enumerate(pattern_properties.items())
            ],
        )

    def parse_item(  # noqa: PLR0911, PLR0912
        self,
        name: str,
        item: JsonSchemaObject,
        path: list[str],
        singular_name: bool = False,  # noqa: FBT001, FBT002
        parent: JsonSchemaObject | None = None,
    ) -> DataType:
        if self.use_title_as_name and item.title:
            name = item.title
            singular_name = False
        if parent and not item.enum and item.has_constraint and (parent.has_constraint or self.field_constraints):
            root_type_path = get_special_path("array", path)
            return self.parse_root_type(
                self.model_resolver.add(
                    root_type_path,
                    name,
                    class_name=True,
                    singular_name=singular_name,
                ).name,
                item,
                root_type_path,
            )
        if item.ref:
            return self.get_ref_data_type(item.ref)
        if item.custom_type_path:
            return self.data_type_manager.get_data_type_from_full_path(item.custom_type_path, is_custom_type=True)
        if item.is_array:
            return self.parse_array_fields(name, item, get_special_path("array", path)).data_type
        if item.discriminator and parent and parent.is_array and (item.oneOf or item.anyOf):
            return self.parse_root_type(name, item, path)
        if item.anyOf:
            return self.data_type(data_types=self.parse_any_of(name, item, get_special_path("anyOf", path)))
        if item.oneOf:
            return self.data_type(data_types=self.parse_one_of(name, item, get_special_path("oneOf", path)))
        if item.allOf:
            all_of_path = get_special_path("allOf", path)
            all_of_path = [self.model_resolver.resolve_ref(all_of_path)]
            return self.parse_all_of(
                self.model_resolver.add(all_of_path, name, singular_name=singular_name, class_name=True).name,
                item,
                all_of_path,
                ignore_duplicate_model=True,
            )
        if item.is_object or item.patternProperties:
            object_path = get_special_path("object", path)
            if item.properties:
                return self.parse_object(name, item, object_path, singular_name=singular_name)
            if item.patternProperties:
                # support only single key dict.
                return self.parse_pattern_properties(name, item.patternProperties, object_path)
            if isinstance(item.additionalProperties, JsonSchemaObject):
                return self.data_type(
                    data_types=[self.parse_item(name, item.additionalProperties, object_path)],
                    is_dict=True,
                )
            return self.data_type_manager.get_data_type(
                Types.object,
            )
        if item.enum:
            if self.should_parse_enum_as_literal(item):
                return self.parse_enum_as_literal(item)
            return self.parse_enum(name, item, get_special_path("enum", path), singular_name=singular_name)
        return self.get_data_type(item)

    def parse_list_item(
        self,
        name: str,
        target_items: list[JsonSchemaObject],
        path: list[str],
        parent: JsonSchemaObject,
        singular_name: bool = True,  # noqa: FBT001, FBT002
    ) -> list[DataType]:
        return [
            self.parse_item(
                name,
                item,
                [*path, str(index)],
                singular_name=singular_name,
                parent=parent,
            )
            for index, item in enumerate(target_items)
        ]

    def parse_array_fields(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        singular_name: bool = True,  # noqa: FBT001, FBT002
    ) -> DataModelFieldBase:
        if self.force_optional_for_required_fields:
            required: bool = False
            nullable: Optional[bool] = None  # noqa: UP045
        else:
            required = not (obj.has_default and self.apply_default_values_for_required_fields)
            if self.strict_nullable:
                nullable = obj.nullable if obj.has_default or required else True
            else:
                required = not obj.nullable and required
                nullable = None
        if isinstance(obj.items, JsonSchemaObject):
            items: list[JsonSchemaObject] = [obj.items]
        elif isinstance(obj.items, list):
            items = obj.items
        else:
            items = []

        data_types: list[DataType] = [
            self.data_type(
                data_types=self.parse_list_item(
                    name,
                    items,
                    path,
                    obj,
                    singular_name=singular_name,
                ),
                is_list=True,
            )
        ]
        # TODO: decide special path word for a combined data model.
        if obj.allOf:
            data_types.append(self.parse_all_of(name, obj, get_special_path("allOf", path)))
        elif obj.is_object:
            data_types.append(self.parse_object(name, obj, get_special_path("object", path)))
        if obj.enum:
            data_types.append(self.parse_enum(name, obj, get_special_path("enum", path)))
        return self.data_model_field_type(
            data_type=self.data_type(data_types=data_types),
            default=obj.default,
            required=required,
            constraints=obj.dict(),
            nullable=nullable,
            strip_default_none=self.strip_default_none,
            extras=self.get_field_extras(obj),
            use_annotated=self.use_annotated,
            use_field_description=self.use_field_description,
            original_name=None,
            has_default=obj.has_default,
        )

    def parse_array(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        original_name: str | None = None,
    ) -> DataType:
        if self.use_title_as_name and obj.title:
            name = obj.title
        reference = self.model_resolver.add(path, name, loaded=True, class_name=True)
        field = self.parse_array_fields(original_name or name, obj, [*path, name])

        if reference in [d.reference for d in field.data_type.all_data_types if d.reference]:
            # self-reference
            field = self.data_model_field_type(
                data_type=self.data_type(
                    data_types=[
                        self.data_type(data_types=field.data_type.data_types[1:], is_list=True),
                        *field.data_type.data_types[1:],
                    ]
                ),
                default=field.default,
                required=field.required,
                constraints=field.constraints,
                nullable=field.nullable,
                strip_default_none=field.strip_default_none,
                extras=field.extras,
                use_annotated=self.use_annotated,
                use_field_description=self.use_field_description,
                original_name=None,
                has_default=field.has_default,
            )

        data_model_root = self.data_model_root_type(
            reference=reference,
            fields=[field],
            custom_base_class=obj.custom_base_path or self.base_class,
            custom_template_dir=self.custom_template_dir,
            extra_template_data=self.extra_template_data,
            path=self.current_source_path,
            description=obj.description if self.use_schema_description else None,
            nullable=obj.type_has_null,
            treat_dot_as_module=self.treat_dot_as_module,
        )
        self.results.append(data_model_root)
        return self.data_type(reference=reference)

    def parse_root_type(  # noqa: PLR0912
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
    ) -> DataType:
        reference: Reference | None = None
        if obj.ref:
            data_type: DataType = self.get_ref_data_type(obj.ref)
        elif obj.custom_type_path:
            data_type = self.data_type_manager.get_data_type_from_full_path(
                obj.custom_type_path, is_custom_type=True
            )  # pragma: no cover
        elif obj.is_array:
            data_type = self.parse_array_fields(
                name, obj, get_special_path("array", path)
            ).data_type  # pragma: no cover
        elif obj.anyOf or obj.oneOf:
            reference = self.model_resolver.add(path, name, loaded=True, class_name=True)
            if obj.anyOf:
                data_types: list[DataType] = self.parse_any_of(name, obj, get_special_path("anyOf", path))
            else:
                data_types = self.parse_one_of(name, obj, get_special_path("oneOf", path))

            if len(data_types) > 1:  # pragma: no cover
                data_type = self.data_type(data_types=data_types)
            elif not data_types:  # pragma: no cover
                return EmptyDataType()
            else:  # pragma: no cover
                data_type = data_types[0]
        elif obj.patternProperties:
            data_type = self.parse_pattern_properties(name, obj.patternProperties, path)
        elif obj.enum:
            if self.should_parse_enum_as_literal(obj):
                data_type = self.parse_enum_as_literal(obj)
            else:  # pragma: no cover
                data_type = self.parse_enum(name, obj, path)
        elif obj.type:
            data_type = self.get_data_type(obj)
        else:
            data_type = self.data_type_manager.get_data_type(
                Types.any,
            )
        if self.force_optional_for_required_fields:
            required: bool = False
        else:
            required = not obj.nullable and not (obj.has_default and self.apply_default_values_for_required_fields)
        if self.use_title_as_name and obj.title:
            name = obj.title
        if not reference:
            reference = self.model_resolver.add(path, name, loaded=True, class_name=True)
        self.set_title(reference.path, obj)
        self.set_additional_properties(reference.path, obj)
        data_model_root_type = self.data_model_root_type(
            reference=reference,
            fields=[
                self.data_model_field_type(
                    data_type=data_type,
                    default=obj.default,
                    required=required,
                    constraints=obj.dict() if self.field_constraints else {},
                    nullable=obj.nullable if self.strict_nullable else None,
                    strip_default_none=self.strip_default_none,
                    extras=self.get_field_extras(obj),
                    use_annotated=self.use_annotated,
                    use_field_description=self.use_field_description,
                    original_name=None,
                    has_default=obj.has_default,
                )
            ],
            custom_base_class=obj.custom_base_path or self.base_class,
            custom_template_dir=self.custom_template_dir,
            extra_template_data=self.extra_template_data,
            path=self.current_source_path,
            nullable=obj.type_has_null,
            treat_dot_as_module=self.treat_dot_as_module,
            default=obj.default if obj.has_default else UNDEFINED,
        )
        self.results.append(data_model_root_type)
        return self.data_type(reference=reference)

    def parse_enum_as_literal(self, obj: JsonSchemaObject) -> DataType:
        return self.data_type(literals=[i for i in obj.enum if i is not None])

    def parse_enum(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
        singular_name: bool = False,  # noqa: FBT001, FBT002
        unique: bool = True,  # noqa: FBT001, FBT002
    ) -> DataType:
        if not unique:  # pragma: no cover
            warn(
                f"{self.__class__.__name__}.parse_enum() ignore `unique` argument."
                f"An object name must be unique."
                f"This argument will be removed in a future version",
                stacklevel=2,
            )
        enum_fields: list[DataModelFieldBase] = []

        if None in obj.enum and obj.type == "string":
            # Nullable is valid in only OpenAPI
            nullable: bool = True
            enum_times = [e for e in obj.enum if e is not None]
        else:
            enum_times = obj.enum
            nullable = False

        exclude_field_names: set[str] = set()

        for i, enum_part in enumerate(enum_times):
            if obj.type == "string" or isinstance(enum_part, str):
                default = f"'{enum_part.translate(escape_characters)}'" if isinstance(enum_part, str) else enum_part
                field_name = obj.x_enum_varnames[i] if obj.x_enum_varnames else str(enum_part)
            else:
                default = enum_part
                if obj.x_enum_varnames:
                    field_name = obj.x_enum_varnames[i]
                else:
                    prefix = obj.type if isinstance(obj.type, str) else type(enum_part).__name__
                    field_name = f"{prefix}_{enum_part}"
            field_name = self.model_resolver.get_valid_field_name(
                field_name, excludes=exclude_field_names, model_type=ModelType.ENUM
            )
            exclude_field_names.add(field_name)
            enum_fields.append(
                self.data_model_field_type(
                    name=field_name,
                    default=default,
                    data_type=self.data_type_manager.get_data_type(
                        Types.any,
                    ),
                    required=True,
                    strip_default_none=self.strip_default_none,
                    has_default=obj.has_default,
                    use_field_description=self.use_field_description,
                    original_name=None,
                )
            )

        def create_enum(reference_: Reference) -> DataType:
            enum = Enum(
                reference=reference_,
                fields=enum_fields,
                path=self.current_source_path,
                description=obj.description if self.use_schema_description else None,
                custom_template_dir=self.custom_template_dir,
                type_=_get_type(obj.type, obj.format) if self.use_subclass_enum and isinstance(obj.type, str) else None,
                default=obj.default if obj.has_default else UNDEFINED,
                treat_dot_as_module=self.treat_dot_as_module,
            )
            self.results.append(enum)
            return self.data_type(reference=reference_)

        if self.use_title_as_name and obj.title:
            name = obj.title
        reference = self.model_resolver.add(
            path,
            name,
            class_name=True,
            singular_name=singular_name,
            singular_name_suffix="Enum",
            loaded=True,
        )

        if not nullable:
            return create_enum(reference)

        enum_reference = self.model_resolver.add(
            [*path, "Enum"],
            f"{reference.name}Enum",
            class_name=True,
            singular_name=singular_name,
            singular_name_suffix="Enum",
            loaded=True,
        )

        data_model_root_type = self.data_model_root_type(
            reference=reference,
            fields=[
                self.data_model_field_type(
                    data_type=create_enum(enum_reference),
                    default=obj.default,
                    required=False,
                    nullable=True,
                    strip_default_none=self.strip_default_none,
                    extras=self.get_field_extras(obj),
                    use_annotated=self.use_annotated,
                    has_default=obj.has_default,
                    use_field_description=self.use_field_description,
                    original_name=None,
                )
            ],
            custom_base_class=obj.custom_base_path or self.base_class,
            custom_template_dir=self.custom_template_dir,
            extra_template_data=self.extra_template_data,
            path=self.current_source_path,
            default=obj.default if obj.has_default else UNDEFINED,
            nullable=obj.type_has_null,
            treat_dot_as_module=self.treat_dot_as_module,
        )
        self.results.append(data_model_root_type)
        return self.data_type(reference=reference)

    def _get_ref_body(self, resolved_ref: str) -> dict[Any, Any]:
        if is_url(resolved_ref):
            return self._get_ref_body_from_url(resolved_ref)
        return self._get_ref_body_from_remote(resolved_ref)

    def _get_ref_body_from_url(self, ref: str) -> dict[Any, Any]:
        # URL Reference: $ref: 'http://path/to/your/resource' Uses the whole document located on the different server.
        return self.remote_object_cache.get_or_put(
            ref, default_factory=lambda key: load_yaml(self._get_text_from_url(key))
        )

    def _get_ref_body_from_remote(self, resolved_ref: str) -> dict[Any, Any]:
        # Remote Reference: $ref: 'document.json' Uses the whole document located on the same server and in
        # the same location. TODO treat edge case
        full_path = self.base_path / resolved_ref

        return self.remote_object_cache.get_or_put(
            str(full_path),
            default_factory=lambda _: load_yaml_from_path(full_path, self.encoding),
        )

    def resolve_ref(self, object_ref: str) -> Reference:
        reference = self.model_resolver.add_ref(object_ref)
        if reference.loaded:
            return reference

        # https://swagger.io/docs/specification/using-ref/
        ref = self.model_resolver.resolve_ref(object_ref)
        if get_ref_type(object_ref) == JSONReference.LOCAL:
            # Local Reference: $ref: '#/definitions/myElement'
            self.reserved_refs[tuple(self.model_resolver.current_root)].add(ref)
            return reference
        if self.model_resolver.is_after_load(ref):
            self.reserved_refs[tuple(ref.split("#")[0].split("/"))].add(ref)
            return reference

        if is_url(ref):
            relative_path, object_path = ref.split("#")
            relative_paths = [relative_path]
            base_path = None
        else:
            if self.model_resolver.is_external_root_ref(ref):
                relative_path, object_path = ref[:-1], ""
            else:
                relative_path, object_path = ref.split("#")
            relative_paths = relative_path.split("/")
            base_path = Path(*relative_paths).parent
        with (
            self.model_resolver.current_base_path_context(base_path),
            self.model_resolver.base_url_context(relative_path),
        ):
            self._parse_file(
                self._get_ref_body(relative_path),
                self.model_resolver.add_ref(ref, resolved=True).name,
                relative_paths,
                object_path.split("/") if object_path else None,
            )
        reference.loaded = True
        return reference

    def parse_ref(self, obj: JsonSchemaObject, path: list[str]) -> None:  # noqa: PLR0912
        if obj.ref:
            self.resolve_ref(obj.ref)
        if obj.items:
            if isinstance(obj.items, JsonSchemaObject):
                self.parse_ref(obj.items, path)
            elif isinstance(obj.items, list):
                for item in obj.items:
                    self.parse_ref(item, path)
        if isinstance(obj.additionalProperties, JsonSchemaObject):
            self.parse_ref(obj.additionalProperties, path)
        if obj.patternProperties:
            for value in obj.patternProperties.values():
                self.parse_ref(value, path)
        for item in obj.anyOf:
            self.parse_ref(item, path)
        for item in obj.allOf:
            self.parse_ref(item, path)
        for item in obj.oneOf:
            self.parse_ref(item, path)
        if obj.properties:
            for property_value in obj.properties.values():
                if isinstance(property_value, JsonSchemaObject):
                    self.parse_ref(property_value, path)

    def parse_id(self, obj: JsonSchemaObject, path: list[str]) -> None:  # noqa: PLR0912
        if obj.id:
            self.model_resolver.add_id(obj.id, path)
        if obj.items:
            if isinstance(obj.items, JsonSchemaObject):
                self.parse_id(obj.items, path)
            elif isinstance(obj.items, list):
                for item in obj.items:
                    self.parse_id(item, path)
        if isinstance(obj.additionalProperties, JsonSchemaObject):
            self.parse_id(obj.additionalProperties, path)
        if obj.patternProperties:
            for value in obj.patternProperties.values():
                self.parse_id(value, path)
        for item in obj.anyOf:
            self.parse_id(item, path)
        for item in obj.allOf:
            self.parse_id(item, path)
        if obj.properties:
            for property_value in obj.properties.values():
                if isinstance(property_value, JsonSchemaObject):
                    self.parse_id(property_value, path)

    @contextmanager
    def root_id_context(self, root_raw: dict[str, Any]) -> Generator[None, None, None]:
        root_id: str | None = root_raw.get("$id")
        previous_root_id: str | None = self.root_id
        self.root_id = root_id or None
        yield
        self.root_id = previous_root_id

    def parse_raw_obj(
        self,
        name: str,
        raw: dict[str, Any],
        path: list[str],
    ) -> None:
        self.parse_obj(name, self.SCHEMA_OBJECT_TYPE.model_validate(raw), path)

    def parse_obj(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
    ) -> None:
        if obj.is_array:
            self.parse_array(name, obj, path)
        elif obj.allOf:
            self.parse_all_of(name, obj, path)
        elif obj.oneOf or obj.anyOf:
            data_type = self.parse_root_type(name, obj, path)
            if isinstance(data_type, EmptyDataType) and obj.properties:
                self.parse_object(name, obj, path)  # pragma: no cover
        elif obj.properties:
            self.parse_object(name, obj, path)
        elif obj.patternProperties:
            self.parse_root_type(name, obj, path)
        elif obj.type == "object":
            self.parse_object(name, obj, path)
        elif obj.enum and not self.should_parse_enum_as_literal(obj):
            self.parse_enum(name, obj, path)
        else:
            self.parse_root_type(name, obj, path)
        self.parse_ref(obj, path)

    def _get_context_source_path_parts(self) -> Iterator[tuple[Source, list[str]]]:
        if isinstance(self.source, list) or (isinstance(self.source, Path) and self.source.is_dir()):
            self.current_source_path = Path()
            self.model_resolver.after_load_files = {
                self.base_path.joinpath(s.path).resolve().as_posix() for s in self.iter_source
            }

        for source in self.iter_source:
            if isinstance(self.source, ParseResult):
                path_parts = self.get_url_path_parts(self.source)
            else:
                path_parts = list(source.path.parts)
            if self.current_source_path is not None:
                self.current_source_path = source.path
            with (
                self.model_resolver.current_base_path_context(source.path.parent),
                self.model_resolver.current_root_context(path_parts),
            ):
                yield source, path_parts

    def parse_raw(self) -> None:
        for source, path_parts in self._get_context_source_path_parts():
            self.raw_obj = load_yaml(source.text)
            if self.raw_obj is None:  # pragma: no cover
                warn(f"{source.path} is empty. Skipping this file", stacklevel=2)
                continue
            if self.custom_class_name_generator:
                obj_name = self.raw_obj.get("title", "Model")
            else:
                if self.class_name:
                    obj_name = self.class_name
                else:
                    # backward compatible
                    obj_name = self.raw_obj.get("title", "Model")
                    if not self.model_resolver.validate_name(obj_name):
                        obj_name = title_to_class_name(obj_name)
                if not self.model_resolver.validate_name(obj_name):
                    raise InvalidClassNameError(obj_name)
            self._parse_file(self.raw_obj, obj_name, path_parts)

        self._resolve_unparsed_json_pointer()

    def _resolve_unparsed_json_pointer(self) -> None:
        model_count: int = len(self.results)
        for source in self.iter_source:
            path_parts = list(source.path.parts)
            reserved_refs = self.reserved_refs.get(tuple(path_parts))
            if not reserved_refs:
                continue
            if self.current_source_path is not None:
                self.current_source_path = source.path

            with (
                self.model_resolver.current_base_path_context(source.path.parent),
                self.model_resolver.current_root_context(path_parts),
            ):
                for reserved_ref in sorted(reserved_refs):
                    if self.model_resolver.add_ref(reserved_ref, resolved=True).loaded:
                        continue
                    # for root model
                    self.raw_obj = load_yaml(source.text)
                    self.parse_json_pointer(self.raw_obj, reserved_ref, path_parts)

        if model_count != len(self.results):
            # New model have been generated. It try to resolve json pointer again.
            self._resolve_unparsed_json_pointer()

    def parse_json_pointer(self, raw: dict[str, Any], ref: str, path_parts: list[str]) -> None:
        path = ref.split("#", 1)[-1]
        if path[0] == "/":  # pragma: no cover
            path = path[1:]
        object_paths = path.split("/")
        models = get_model_by_path(raw, object_paths)
        model_name = object_paths[-1]

        self.parse_raw_obj(model_name, models, [*path_parts, f"#/{object_paths[0]}", *object_paths[1:]])

    def _parse_file(  # noqa: PLR0912
        self,
        raw: dict[str, Any],
        obj_name: str,
        path_parts: list[str],
        object_paths: list[str] | None = None,
    ) -> None:
        object_paths = [o for o in object_paths or [] if o]
        path = [*path_parts, f"#/{object_paths[0]}", *object_paths[1:]] if object_paths else path_parts
        with self.model_resolver.current_root_context(path_parts):
            obj_name = self.model_resolver.add(path, obj_name, unique=False, class_name=True).name
            with self.root_id_context(raw):
                # Some jsonschema docs include attribute self to have include version details
                raw.pop("self", None)
                # parse $id before parsing $ref
                root_obj = self.SCHEMA_OBJECT_TYPE.parse_obj(raw)
                self.parse_id(root_obj, path_parts)
                definitions: dict[Any, Any] | None = None
                _schema_path = ""
                for _schema_path, split_schema_path in self.schema_paths:
                    try:
                        definitions = get_model_by_path(raw, split_schema_path)
                        if definitions:
                            break
                    except KeyError:
                        continue
                if definitions is None:
                    definitions = {}

                for key, model in definitions.items():
                    obj = self.SCHEMA_OBJECT_TYPE.parse_obj(model)
                    self.parse_id(obj, [*path_parts, _schema_path, key])

                if object_paths:
                    models = get_model_by_path(raw, object_paths)
                    model_name = object_paths[-1]
                    self.parse_obj(model_name, self.SCHEMA_OBJECT_TYPE.parse_obj(models), path)
                else:
                    self.parse_obj(obj_name, root_obj, path_parts or ["#"])
                for key, model in definitions.items():
                    path = [*path_parts, _schema_path, key]
                    reference = self.model_resolver.get(path)
                    if not reference or not reference.loaded:
                        self.parse_raw_obj(key, model, path)

                key = tuple(path_parts)
                reserved_refs = set(self.reserved_refs.get(key) or [])
                while reserved_refs:
                    for reserved_path in sorted(reserved_refs):
                        reference = self.model_resolver.get(reserved_path)
                        if not reference or reference.loaded:
                            continue
                        object_paths = reserved_path.split("#/", 1)[-1].split("/")
                        path = reserved_path.split("/")
                        models = get_model_by_path(raw, object_paths)
                        model_name = object_paths[-1]
                        self.parse_obj(model_name, self.SCHEMA_OBJECT_TYPE.parse_obj(models), path)
                    previous_reserved_refs = reserved_refs
                    reserved_refs = set(self.reserved_refs.get(key) or [])
                    if previous_reserved_refs == reserved_refs:
                        break
