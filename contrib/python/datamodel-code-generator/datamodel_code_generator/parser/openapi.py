from __future__ import annotations

import re
from collections import defaultdict
from enum import Enum
from re import Pattern
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypeVar, Union
from warnings import warn

from pydantic import Field

from datamodel_code_generator import (
    Error,
    LiteralType,
    OpenAPIScope,
    PythonVersion,
    PythonVersionMin,
    load_yaml,
    snooper_to_methods,
)
from datamodel_code_generator.format import DEFAULT_FORMATTERS, DatetimeClassType, Formatter
from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model import pydantic as pydantic_model
from datamodel_code_generator.parser import DefaultPutDict  # noqa: TC001 # needed for type check
from datamodel_code_generator.parser.base import get_special_path
from datamodel_code_generator.parser.jsonschema import (
    JsonSchemaObject,
    JsonSchemaParser,
    get_model_by_path,
)
from datamodel_code_generator.reference import snake_to_upper_camel
from datamodel_code_generator.types import (
    DataType,
    DataTypeManager,
    EmptyDataType,
    StrictTypes,
)
from datamodel_code_generator.util import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from pathlib import Path
    from urllib.parse import ParseResult


RE_APPLICATION_JSON_PATTERN: Pattern[str] = re.compile(r"^application/.*json$")

OPERATION_NAMES: list[str] = [
    "get",
    "put",
    "post",
    "delete",
    "patch",
    "head",
    "options",
    "trace",
]


class ParameterLocation(Enum):
    query = "query"
    header = "header"
    path = "path"
    cookie = "cookie"


BaseModelT = TypeVar("BaseModelT", bound=BaseModel)


class ReferenceObject(BaseModel):
    ref: str = Field(..., alias="$ref")


class ExampleObject(BaseModel):
    summary: Optional[str] = None  # noqa: UP045
    description: Optional[str] = None  # noqa: UP045
    value: Any = None
    externalValue: Optional[str] = None  # noqa: N815, UP045


class MediaObject(BaseModel):
    schema_: Optional[Union[ReferenceObject, JsonSchemaObject]] = Field(None, alias="schema")  # noqa: UP007, UP045
    example: Any = None
    examples: Optional[Union[str, ReferenceObject, ExampleObject]] = None  # noqa: UP007, UP045


class ParameterObject(BaseModel):
    name: Optional[str] = None  # noqa: UP045
    in_: Optional[ParameterLocation] = Field(None, alias="in")  # noqa: UP045
    description: Optional[str] = None  # noqa: UP045
    required: bool = False
    deprecated: bool = False
    schema_: Optional[JsonSchemaObject] = Field(None, alias="schema")  # noqa: UP045
    example: Any = None
    examples: Optional[Union[str, ReferenceObject, ExampleObject]] = None  # noqa: UP007, UP045
    content: dict[str, MediaObject] = {}  # noqa: RUF012


class HeaderObject(BaseModel):
    description: Optional[str] = None  # noqa: UP045
    required: bool = False
    deprecated: bool = False
    schema_: Optional[JsonSchemaObject] = Field(None, alias="schema")  # noqa: UP045
    example: Any = None
    examples: Optional[Union[str, ReferenceObject, ExampleObject]] = None  # noqa: UP007, UP045
    content: dict[str, MediaObject] = {}  # noqa: RUF012


class RequestBodyObject(BaseModel):
    description: Optional[str] = None  # noqa: UP045
    content: dict[str, MediaObject] = {}  # noqa: RUF012
    required: bool = False


class ResponseObject(BaseModel):
    description: Optional[str] = None  # noqa: UP045
    headers: dict[str, ParameterObject] = {}  # noqa: RUF012
    content: dict[Union[str, int], MediaObject] = {}  # noqa: RUF012, UP007


class Operation(BaseModel):
    tags: list[str] = []  # noqa: RUF012
    summary: Optional[str] = None  # noqa: UP045
    description: Optional[str] = None  # noqa: UP045
    operationId: Optional[str] = None  # noqa: N815, UP045
    parameters: list[Union[ReferenceObject, ParameterObject]] = []  # noqa: RUF012, UP007
    requestBody: Optional[Union[ReferenceObject, RequestBodyObject]] = None  # noqa: N815, UP007, UP045
    responses: dict[Union[str, int], Union[ReferenceObject, ResponseObject]] = {}  # noqa: RUF012, UP007
    deprecated: bool = False


class ComponentsObject(BaseModel):
    schemas: dict[str, Union[ReferenceObject, JsonSchemaObject]] = {}  # noqa: RUF012, UP007
    responses: dict[str, Union[ReferenceObject, ResponseObject]] = {}  # noqa: RUF012, UP007
    examples: dict[str, Union[ReferenceObject, ExampleObject]] = {}  # noqa: RUF012, UP007
    requestBodies: dict[str, Union[ReferenceObject, RequestBodyObject]] = {}  # noqa: N815, RUF012, UP007
    headers: dict[str, Union[ReferenceObject, HeaderObject]] = {}  # noqa: RUF012, UP007


@snooper_to_methods()
class OpenAPIParser(JsonSchemaParser):
    SCHEMA_PATHS: ClassVar[list[str]] = ["#/components/schemas"]

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
        allow_extra_fields: bool = False,
        extra_fields: str | None = None,
        apply_default_values_for_required_fields: bool = False,
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
        openapi_scopes: list[OpenAPIScope] | None = None,
        include_path_parameters: bool = False,
        wrap_string_literal: bool | None = False,
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
        self.open_api_scopes: list[OpenAPIScope] = openapi_scopes or [OpenAPIScope.Schemas]
        self.include_path_parameters: bool = include_path_parameters

    def get_ref_model(self, ref: str) -> dict[str, Any]:
        ref_file, ref_path = self.model_resolver.resolve_ref(ref).split("#", 1)
        ref_body = self._get_ref_body(ref_file) if ref_file else self.raw_obj
        return get_model_by_path(ref_body, ref_path.split("/")[1:])

    def get_data_type(self, obj: JsonSchemaObject) -> DataType:
        # OpenAPI 3.0 doesn't allow `null` in the `type` field and list of types
        # https://swagger.io/docs/specification/data-models/data-types/#null
        # OpenAPI 3.1 does allow `null` in the `type` field and is equivalent to
        # a `nullable` flag on the property itself
        if obj.nullable and self.strict_nullable and isinstance(obj.type, str):
            obj.type = [obj.type, "null"]

        return super().get_data_type(obj)

    def resolve_object(self, obj: ReferenceObject | BaseModelT, object_type: type[BaseModelT]) -> BaseModelT:
        if isinstance(obj, ReferenceObject):
            ref_obj = self.get_ref_model(obj.ref)
            return object_type.parse_obj(ref_obj)
        return obj

    def parse_schema(
        self,
        name: str,
        obj: JsonSchemaObject,
        path: list[str],
    ) -> DataType:
        if obj.is_array:
            data_type = self.parse_array(name, obj, [*path, name])
        elif obj.allOf:  # pragma: no cover
            data_type = self.parse_all_of(name, obj, path)
        elif obj.oneOf or obj.anyOf:  # pragma: no cover
            data_type = self.parse_root_type(name, obj, path)
            if isinstance(data_type, EmptyDataType) and obj.properties:
                self.parse_object(name, obj, path)
        elif obj.is_object:
            data_type = self.parse_object(name, obj, path)
        elif obj.enum:  # pragma: no cover
            data_type = self.parse_enum(name, obj, path)
        elif obj.ref:  # pragma: no cover
            data_type = self.get_ref_data_type(obj.ref)
        else:
            data_type = self.get_data_type(obj)
        self.parse_ref(obj, path)
        return data_type

    def parse_request_body(
        self,
        name: str,
        request_body: RequestBodyObject,
        path: list[str],
    ) -> dict[str, DataType]:
        data_types: dict[str, DataType] = {}
        for (
            media_type,
            media_obj,
        ) in request_body.content.items():
            if isinstance(media_obj.schema_, JsonSchemaObject):
                data_types[media_type] = self.parse_schema(name, media_obj.schema_, [*path, media_type])
            elif media_obj.schema_ is not None:
                data_types[media_type] = self.get_ref_data_type(media_obj.schema_.ref)
        return data_types

    def parse_responses(
        self,
        name: str,
        responses: dict[str | int, ReferenceObject | ResponseObject],
        path: list[str],
    ) -> dict[str | int, dict[str, DataType]]:
        data_types: defaultdict[str | int, dict[str, DataType]] = defaultdict(dict)
        for status_code, detail in responses.items():
            if isinstance(detail, ReferenceObject):
                if not detail.ref:  # pragma: no cover
                    continue
                ref_model = self.get_ref_model(detail.ref)
                content = {k: MediaObject.parse_obj(v) for k, v in ref_model.get("content", {}).items()}
            else:
                content = detail.content

            if self.allow_responses_without_content and not content:
                data_types[status_code]["application/json"] = DataType(type="None")

            for content_type, obj in content.items():
                object_schema = obj.schema_
                if not object_schema:  # pragma: no cover
                    continue
                if isinstance(object_schema, JsonSchemaObject):
                    data_types[status_code][content_type] = self.parse_schema(  # pyright: ignore[reportArgumentType]
                        name,
                        object_schema,
                        [*path, str(status_code), content_type],  # pyright: ignore[reportArgumentType]
                    )
                else:
                    data_types[status_code][content_type] = self.get_ref_data_type(  # pyright: ignore[reportArgumentType]
                        object_schema.ref
                    )

        return data_types

    @classmethod
    def parse_tags(
        cls,
        name: str,  # noqa: ARG003
        tags: list[str],
        path: list[str],  # noqa: ARG003
    ) -> list[str]:
        return tags

    @classmethod
    def _get_model_name(cls, path_name: str, method: str, suffix: str) -> str:
        camel_path_name = snake_to_upper_camel(path_name.replace("/", "_"))
        return f"{camel_path_name}{method.capitalize()}{suffix}"

    def parse_all_parameters(
        self,
        name: str,
        parameters: list[ReferenceObject | ParameterObject],
        path: list[str],
    ) -> DataType | None:
        fields: list[DataModelFieldBase] = []
        exclude_field_names: set[str] = set()
        reference = self.model_resolver.add(path, name, class_name=True, unique=True)
        for parameter_ in parameters:
            parameter = self.resolve_object(parameter_, ParameterObject)
            parameter_name = parameter.name
            if (
                not parameter_name
                or parameter.in_ not in {ParameterLocation.query, ParameterLocation.path}
                or (parameter.in_ == ParameterLocation.path and not self.include_path_parameters)
            ):
                continue

            if any(field.original_name == parameter_name for field in fields):
                msg = f"Parameter name '{parameter_name}' is used more than once."
                raise Exception(msg)  # noqa: TRY002

            field_name, alias = self.model_resolver.get_valid_field_name_and_alias(
                field_name=parameter_name, excludes=exclude_field_names
            )
            if parameter.schema_:
                fields.append(
                    self.get_object_field(
                        field_name=field_name,
                        field=parameter.schema_,
                        field_type=self.parse_item(field_name, parameter.schema_, [*path, name, parameter_name]),
                        original_field_name=parameter_name,
                        required=parameter.required,
                        alias=alias,
                    )
                )
            else:
                data_types: list[DataType] = []
                object_schema: JsonSchemaObject | None = None
                for (
                    media_type,
                    media_obj,
                ) in parameter.content.items():
                    if not media_obj.schema_:
                        continue
                    object_schema = self.resolve_object(media_obj.schema_, JsonSchemaObject)
                    data_types.append(
                        self.parse_item(
                            field_name,
                            object_schema,
                            [*path, name, parameter_name, media_type],
                        )
                    )

                if not data_types:
                    continue
                if len(data_types) == 1:
                    data_type = data_types[0]
                else:
                    data_type = self.data_type(data_types=data_types)
                    # multiple data_type parse as non-constraints field
                    object_schema = None
                fields.append(
                    self.data_model_field_type(
                        name=field_name,
                        default=object_schema.default if object_schema else None,
                        data_type=data_type,
                        required=parameter.required,
                        alias=alias,
                        constraints=object_schema.dict()
                        if object_schema and self.is_constraints_field(object_schema)
                        else None,
                        nullable=object_schema.nullable
                        if object_schema and self.strict_nullable and (object_schema.has_default or parameter.required)
                        else None,
                        strip_default_none=self.strip_default_none,
                        extras=self.get_field_extras(object_schema) if object_schema else {},
                        use_annotated=self.use_annotated,
                        use_field_description=self.use_field_description,
                        use_default_kwarg=self.use_default_kwarg,
                        original_name=parameter_name,
                        has_default=object_schema.has_default if object_schema else False,
                        type_has_null=object_schema.type_has_null if object_schema else None,
                    )
                )

        if OpenAPIScope.Parameters in self.open_api_scopes and fields:
            # Using _create_data_model from parent class JsonSchemaParser
            # This method automatically adds frozen=True for DataClass types
            self.results.append(
                self._create_data_model(
                    fields=fields,
                    reference=reference,
                    custom_base_class=self.base_class,
                    custom_template_dir=self.custom_template_dir,
                    keyword_only=self.keyword_only,
                    treat_dot_as_module=self.treat_dot_as_module,
                )
            )
            return self.data_type(reference=reference)

        return None

    def parse_operation(
        self,
        raw_operation: dict[str, Any],
        path: list[str],
    ) -> None:
        operation = Operation.parse_obj(raw_operation)
        path_name, method = path[-2:]
        if self.use_operation_id_as_name:
            if not operation.operationId:
                msg = (
                    f"All operations must have an operationId when --use_operation_id_as_name is set."
                    f"The following path was missing an operationId: {path_name}"
                )
                raise Error(msg)
            path_name = operation.operationId
            method = ""
        self.parse_all_parameters(
            self._get_model_name(
                path_name, method, suffix="Parameters" if self.include_path_parameters else "ParametersQuery"
            ),
            operation.parameters,
            [*path, "parameters"],
        )
        if operation.requestBody:
            if isinstance(operation.requestBody, ReferenceObject):
                ref_model = self.get_ref_model(operation.requestBody.ref)
                request_body = RequestBodyObject.parse_obj(ref_model)
            else:
                request_body = operation.requestBody
            self.parse_request_body(
                name=self._get_model_name(path_name, method, suffix="Request"),
                request_body=request_body,
                path=[*path, "requestBody"],
            )
        self.parse_responses(
            name=self._get_model_name(path_name, method, suffix="Response"),
            responses=operation.responses,
            path=[*path, "responses"],
        )
        if OpenAPIScope.Tags in self.open_api_scopes:
            self.parse_tags(
                name=self._get_model_name(path_name, method, suffix="Tags"),
                tags=operation.tags,
                path=[*path, "tags"],
            )

    def parse_raw(self) -> None:  # noqa: PLR0912
        for source, path_parts in self._get_context_source_path_parts():  # noqa: PLR1702
            if self.validation:
                warn(
                    "Deprecated: `--validation` option is deprecated. the option will be removed in a future "
                    "release. please use another tool to validate OpenAPI.\n",
                    stacklevel=2,
                )

                try:
                    from prance import BaseParser  # noqa: PLC0415

                    BaseParser(
                        spec_string=source.text,
                        backend="openapi-spec-validator",
                        encoding=self.encoding,
                    )
                except ImportError:  # pragma: no cover
                    warn(
                        "Warning: Validation was skipped for OpenAPI. `prance` or `openapi-spec-validator` are not "
                        "installed.\n"
                        "To use --validation option after datamodel-code-generator 0.24.0, Please run `$pip install "
                        "'datamodel-code-generator[validation]'`.\n",
                        stacklevel=2,
                    )

            specification: dict[str, Any] = load_yaml(source.text)
            self.raw_obj = specification
            schemas: dict[Any, Any] = specification.get("components", {}).get("schemas", {})
            security: list[dict[str, list[str]]] | None = specification.get("security")
            if OpenAPIScope.Schemas in self.open_api_scopes:
                for (
                    obj_name,
                    raw_obj,
                ) in schemas.items():
                    self.parse_raw_obj(
                        obj_name,
                        raw_obj,
                        [*path_parts, "#/components", "schemas", obj_name],
                    )
            if OpenAPIScope.Paths in self.open_api_scopes:
                paths: dict[str, dict[str, Any]] = specification.get("paths", {})
                parameters: list[dict[str, Any]] = [
                    self._get_ref_body(p["$ref"]) if "$ref" in p else p
                    for p in paths.get("parameters", [])
                    if isinstance(p, dict)
                ]
                paths_path = [*path_parts, "#/paths"]
                for path_name, methods_ in paths.items():
                    # Resolve path items if applicable
                    methods = self.get_ref_model(methods_["$ref"]) if "$ref" in methods_ else methods_
                    paths_parameters = parameters.copy()
                    if "parameters" in methods:
                        paths_parameters.extend(methods["parameters"])
                    relative_path_name = path_name[1:]
                    if relative_path_name:
                        path = [*paths_path, relative_path_name]
                    else:  # pragma: no cover
                        path = get_special_path("root", paths_path)
                    for operation_name, raw_operation in methods.items():
                        if operation_name not in OPERATION_NAMES:
                            continue
                        if paths_parameters:
                            if "parameters" in raw_operation:  # pragma: no cover
                                raw_operation["parameters"].extend(paths_parameters)
                            else:
                                raw_operation["parameters"] = paths_parameters
                        if security is not None and "security" not in raw_operation:
                            raw_operation["security"] = security
                        self.parse_operation(
                            raw_operation,
                            [*path, operation_name],
                        )

        self._resolve_unparsed_json_pointer()
