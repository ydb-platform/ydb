"""
Main function.
"""

from __future__ import annotations

import json
import signal
import sys
import warnings
from collections import defaultdict
from collections.abc import Sequence  # noqa: TC003  # pydantic needs it
from enum import IntEnum
from io import TextIOBase
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast
from urllib.parse import ParseResult, urlparse

import argcomplete
import black
from pydantic import BaseModel

if TYPE_CHECKING:
    from argparse import Namespace

    from typing_extensions import Self

from datamodel_code_generator import (
    DataModelType,
    Error,
    InputFileType,
    InvalidClassNameError,
    OpenAPIScope,
    enable_debug_message,
    generate,
)
from datamodel_code_generator.arguments import DEFAULT_ENCODING, arg_parser, namespace
from datamodel_code_generator.format import (
    DEFAULT_FORMATTERS,
    DatetimeClassType,
    Formatter,
    PythonVersion,
    PythonVersionMin,
    is_supported_in_black,
)
from datamodel_code_generator.model.pydantic_v2 import UnionMode  # noqa: TC001 # needed for pydantic
from datamodel_code_generator.parser import LiteralType  # noqa: TC001 # needed for pydantic
from datamodel_code_generator.reference import is_url
from datamodel_code_generator.types import StrictTypes  # noqa: TC001 # needed for pydantic
from datamodel_code_generator.util import (
    PYDANTIC_V2,
    ConfigDict,
    Model,
    field_validator,
    load_toml,
    model_validator,
)


class Exit(IntEnum):
    """Exit reasons."""

    OK = 0
    ERROR = 1
    KeyboardInterrupt = 2


def sig_int_handler(_: int, __: Any) -> None:  # pragma: no cover
    sys.exit(Exit.OK)


signal.signal(signal.SIGINT, sig_int_handler)


class Config(BaseModel):
    if PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)  # pyright: ignore[reportAssignmentType]

        def get(self, item: str) -> Any:
            return getattr(self, item)

        def __getitem__(self, item: str) -> Any:
            return self.get(item)

        if TYPE_CHECKING:

            @classmethod
            def get_fields(cls) -> dict[str, Any]: ...

        else:

            @classmethod
            def parse_obj(cls: type[Model], obj: Any) -> Model:
                return cls.model_validate(obj)

            @classmethod
            def get_fields(cls) -> dict[str, Any]:
                return cls.model_fields

    else:

        class Config:
            # Pydantic 1.5.1 doesn't support validate_assignment correctly
            arbitrary_types_allowed = (TextIOBase,)

        if not TYPE_CHECKING:

            @classmethod
            def get_fields(cls) -> dict[str, Any]:
                return cls.__fields__

    @field_validator("aliases", "extra_template_data", "custom_formatters_kwargs", mode="before")
    def validate_file(cls, value: Any) -> TextIOBase | None:  # noqa: N805
        if value is None:  # pragma: no cover
            return value

        path = Path(value)
        if path.is_file():
            return cast("TextIOBase", path.expanduser().resolve().open("rt"))

        msg = f"A file was expected but {value} is not a file."
        raise Error(msg)  # pragma: no cover

    @field_validator(
        "input",
        "output",
        "custom_template_dir",
        "custom_file_header_path",
        mode="before",
    )
    def validate_path(cls, value: Any) -> Path | None:  # noqa: N805
        if value is None or isinstance(value, Path):
            return value  # pragma: no cover
        return Path(value).expanduser().resolve()

    @field_validator("url", mode="before")
    def validate_url(cls, value: Any) -> ParseResult | None:  # noqa: N805
        if isinstance(value, str) and is_url(value):  # pragma: no cover
            return urlparse(value)
        if value is None:  # pragma: no cover
            return None
        msg = f"This protocol doesn't support only http/https. --input={value}"
        raise Error(msg)  # pragma: no cover

    # Pydantic 1.5.1 doesn't support each_item=True correctly
    @field_validator("http_headers", mode="before")
    def validate_http_headers(cls, value: Any) -> list[tuple[str, str]] | None:  # noqa: N805
        def validate_each_item(each_item: Any) -> tuple[str, str]:
            if isinstance(each_item, str):  # pragma: no cover
                try:
                    field_name, field_value = each_item.split(":", maxsplit=1)
                    return field_name, field_value.lstrip()
                except ValueError as exc:
                    msg = f"Invalid http header: {each_item!r}"
                    raise Error(msg) from exc
            return each_item  # pragma: no cover

        if isinstance(value, list):
            return [validate_each_item(each_item) for each_item in value]
        return value  # pragma: no cover

    @field_validator("http_query_parameters", mode="before")
    def validate_http_query_parameters(cls, value: Any) -> list[tuple[str, str]] | None:  # noqa: N805
        def validate_each_item(each_item: Any) -> tuple[str, str]:
            if isinstance(each_item, str):  # pragma: no cover
                try:
                    field_name, field_value = each_item.split("=", maxsplit=1)
                    return field_name, field_value.lstrip()
                except ValueError as exc:
                    msg = f"Invalid http query parameter: {each_item!r}"
                    raise Error(msg) from exc
            return each_item  # pragma: no cover

        if isinstance(value, list):
            return [validate_each_item(each_item) for each_item in value]
        return value  # pragma: no cover

    @model_validator(mode="before")
    def validate_additional_imports(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
        additional_imports = values.get("additional_imports")
        if additional_imports is not None:
            values["additional_imports"] = additional_imports.split(",")
        return values

    @model_validator(mode="before")
    def validate_custom_formatters(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
        custom_formatters = values.get("custom_formatters")
        if custom_formatters is not None:
            values["custom_formatters"] = custom_formatters.split(",")
        return values

    __validate_output_datetime_class_err: ClassVar[str] = (
        '`--output-datetime-class` only allows "datetime" for '
        f"`--output-model-type` {DataModelType.DataclassesDataclass.value}"
    )

    __validate_original_field_name_delimiter_err: ClassVar[str] = (
        "`--original-field-name-delimiter` can not be used without `--snake-case-field`."
    )

    __validate_custom_file_header_err: ClassVar[str] = (
        "`--custom_file_header_path` can not be used with `--custom_file_header`."
    )
    __validate_keyword_only_err: ClassVar[str] = (
        f"`--keyword-only` requires `--target-python-version` {PythonVersion.PY_310.value} or higher."
    )

    if PYDANTIC_V2:

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_output_datetime_class(self: Self) -> Self:  # pyright: ignore[reportRedeclaration]
            datetime_class_type: DatetimeClassType | None = self.output_datetime_class
            if (
                datetime_class_type
                and datetime_class_type is not DatetimeClassType.Datetime
                and self.output_model_type == DataModelType.DataclassesDataclass
            ):
                raise Error(self.__validate_output_datetime_class_err)
            return self

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_original_field_name_delimiter(self: Self) -> Self:  # pyright: ignore[reportRedeclaration]
            if self.original_field_name_delimiter is not None and not self.snake_case_field:
                raise Error(self.__validate_original_field_name_delimiter_err)
            return self

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_custom_file_header(self: Self) -> Self:  # pyright: ignore[reportRedeclaration]
            if self.custom_file_header and self.custom_file_header_path:
                raise Error(self.__validate_custom_file_header_err)
            return self

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_keyword_only(self: Self) -> Self:  # pyright: ignore[reportRedeclaration]
            output_model_type: DataModelType = self.output_model_type
            python_target: PythonVersion = self.target_python_version
            if (
                self.keyword_only
                and output_model_type == DataModelType.DataclassesDataclass
                and not python_target.has_kw_only_dataclass
            ):
                raise Error(self.__validate_keyword_only_err)
            return self

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_root(self: Self) -> Self:  # pyright: ignore[reportRedeclaration]
            if self.use_annotated:
                self.field_constraints = True
            return self

    else:

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_output_datetime_class(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
            datetime_class_type: DatetimeClassType | None = values.get("output_datetime_class")
            if (
                datetime_class_type
                and datetime_class_type is not DatetimeClassType.Datetime
                and values.get("output_model_type") == DataModelType.DataclassesDataclass
            ):
                raise Error(cls.__validate_output_datetime_class_err)
            return values

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_original_field_name_delimiter(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
            if values.get("original_field_name_delimiter") is not None and not values.get("snake_case_field"):
                raise Error(cls.__validate_original_field_name_delimiter_err)
            return values

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_custom_file_header(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
            if values.get("custom_file_header") and values.get("custom_file_header_path"):
                raise Error(cls.__validate_custom_file_header_err)
            return values

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_keyword_only(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
            output_model_type: DataModelType = cast("DataModelType", values.get("output_model_type"))
            python_target: PythonVersion = cast("PythonVersion", values.get("target_python_version"))
            if (
                values.get("keyword_only")
                and output_model_type == DataModelType.DataclassesDataclass
                and not python_target.has_kw_only_dataclass
            ):
                raise Error(cls.__validate_keyword_only_err)
            return values

        @model_validator()  # pyright: ignore[reportArgumentType]
        def validate_root(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: N805
            if values.get("use_annotated"):
                values["field_constraints"] = True
            return values

    input: Optional[Union[Path, str]] = None  # noqa: UP007, UP045
    input_file_type: InputFileType = InputFileType.Auto
    output_model_type: DataModelType = DataModelType.PydanticBaseModel
    output: Optional[Path] = None  # noqa: UP045
    debug: bool = False
    disable_warnings: bool = False
    target_python_version: PythonVersion = PythonVersionMin
    base_class: str = ""
    additional_imports: Optional[list[str]] = None  # noqa: UP045
    custom_template_dir: Optional[Path] = None  # noqa: UP045
    extra_template_data: Optional[TextIOBase] = None  # noqa: UP045
    validation: bool = False
    field_constraints: bool = False
    snake_case_field: bool = False
    strip_default_none: bool = False
    aliases: Optional[TextIOBase] = None  # noqa: UP045
    disable_timestamp: bool = False
    enable_version_header: bool = False
    allow_population_by_field_name: bool = False
    allow_extra_fields: bool = False
    extra_fields: Optional[str] = None  # noqa: UP045
    use_default: bool = False
    force_optional: bool = False
    class_name: Optional[str] = None  # noqa: UP045
    use_standard_collections: bool = False
    use_schema_description: bool = False
    use_field_description: bool = False
    use_default_kwarg: bool = False
    reuse_model: bool = False
    encoding: str = DEFAULT_ENCODING
    enum_field_as_literal: Optional[LiteralType] = None  # noqa: UP045
    use_one_literal_as_default: bool = False
    set_default_enum_member: bool = False
    use_subclass_enum: bool = False
    strict_nullable: bool = False
    use_generic_container_types: bool = False
    use_union_operator: bool = False
    enable_faux_immutability: bool = False
    url: Optional[ParseResult] = None  # noqa: UP045
    disable_appending_item_suffix: bool = False
    strict_types: list[StrictTypes] = []
    empty_enum_field_name: Optional[str] = None  # noqa: UP045
    field_extra_keys: Optional[set[str]] = None  # noqa: UP045
    field_include_all_keys: bool = False
    field_extra_keys_without_x_prefix: Optional[set[str]] = None  # noqa: UP045
    openapi_scopes: Optional[list[OpenAPIScope]] = [OpenAPIScope.Schemas]  # noqa: UP045
    include_path_parameters: bool = False
    wrap_string_literal: Optional[bool] = None  # noqa: UP045
    use_title_as_name: bool = False
    use_operation_id_as_name: bool = False
    use_unique_items_as_set: bool = False
    http_headers: Optional[Sequence[tuple[str, str]]] = None  # noqa: UP045
    http_ignore_tls: bool = False
    use_annotated: bool = False
    use_non_positive_negative_number_constrained_types: bool = False
    original_field_name_delimiter: Optional[str] = None  # noqa: UP045
    use_double_quotes: bool = False
    collapse_root_models: bool = False
    special_field_name_prefix: Optional[str] = None  # noqa: UP045
    remove_special_field_name_prefix: bool = False
    capitalise_enum_members: bool = False
    keep_model_order: bool = False
    custom_file_header: Optional[str] = None  # noqa: UP045
    custom_file_header_path: Optional[Path] = None  # noqa: UP045
    custom_formatters: Optional[list[str]] = None  # noqa: UP045
    custom_formatters_kwargs: Optional[TextIOBase] = None  # noqa: UP045
    use_pendulum: bool = False
    http_query_parameters: Optional[Sequence[tuple[str, str]]] = None  # noqa: UP045
    treat_dot_as_module: bool = False
    use_exact_imports: bool = False
    union_mode: Optional[UnionMode] = None  # noqa: UP045
    output_datetime_class: Optional[DatetimeClassType] = None  # noqa: UP045
    keyword_only: bool = False
    frozen_dataclasses: bool = False
    no_alias: bool = False
    formatters: list[Formatter] = DEFAULT_FORMATTERS
    parent_scoped_naming: bool = False
    disable_future_imports: bool = False

    def merge_args(self, args: Namespace) -> None:
        set_args = {f: getattr(args, f) for f in self.get_fields() if getattr(args, f) is not None}

        if set_args.get("output_model_type") == DataModelType.MsgspecStruct.value:
            set_args["use_annotated"] = True

        if set_args.get("use_annotated"):
            set_args["field_constraints"] = True

        parsed_args = Config.parse_obj(set_args)
        for field_name in set_args:
            setattr(self, field_name, getattr(parsed_args, field_name))


def _get_pyproject_toml_config(source: Path) -> dict[str, Any]:
    """Find and return the [tool.datamodel-codgen] section of the closest
    pyproject.toml if it exists.
    """

    current_path = source
    while current_path != current_path.parent:
        if (current_path / "pyproject.toml").is_file():
            pyproject_toml = load_toml(current_path / "pyproject.toml")
            if "datamodel-codegen" in pyproject_toml.get("tool", {}):
                pyproject_config = pyproject_toml["tool"]["datamodel-codegen"]
                # Convert options from kebap- to snake-case
                pyproject_config = {k.replace("-", "_"): v for k, v in pyproject_config.items()}
                # Replace US-american spelling if present (ignore if british spelling is present)
                if "capitalize_enum_members" in pyproject_config and "capitalise_enum_members" not in pyproject_config:
                    pyproject_config["capitalise_enum_members"] = pyproject_config.pop("capitalize_enum_members")
                return pyproject_config

        if (current_path / ".git").exists():
            # Stop early if we see a git repository root.
            return {}

        current_path = current_path.parent
    return {}


def main(args: Sequence[str] | None = None) -> Exit:  # noqa: PLR0911, PLR0912, PLR0915
    """Main function."""

    # add cli completion support
    argcomplete.autocomplete(arg_parser)

    if args is None:  # pragma: no cover
        args = sys.argv[1:]

    arg_parser.parse_args(args, namespace=namespace)

    if namespace.version:
        from datamodel_code_generator import get_version  # noqa: PLC0415

        print(get_version())  # noqa: T201
        sys.exit(0)

    pyproject_config = _get_pyproject_toml_config(Path.cwd())

    try:
        config = Config.parse_obj(pyproject_config)
        config.merge_args(namespace)
    except Error as e:
        print(e.message, file=sys.stderr)  # noqa: T201
        return Exit.ERROR

    if not config.input and not config.url and sys.stdin.isatty():
        print(  # noqa: T201
            "Not Found Input: require `stdin` or arguments `--input` or `--url`",
            file=sys.stderr,
        )
        arg_parser.print_help()
        return Exit.ERROR

    if not is_supported_in_black(config.target_python_version):  # pragma: no cover
        print(  # noqa: T201
            f"Installed black doesn't support Python version {config.target_python_version.value}.\n"
            f"You have to install a newer black.\n"
            f"Installed black version: {black.__version__}",
            file=sys.stderr,
        )
        return Exit.ERROR

    if config.debug:  # pragma: no cover
        enable_debug_message()

    if config.disable_warnings:
        warnings.simplefilter("ignore")
    extra_template_data: defaultdict[str, dict[str, Any]] | None
    if config.extra_template_data is None:
        extra_template_data = None
    else:
        with config.extra_template_data as data:
            try:
                extra_template_data = json.load(data, object_hook=lambda d: defaultdict(dict, **d))
            except json.JSONDecodeError as e:
                print(f"Unable to load extra template data: {e}", file=sys.stderr)  # noqa: T201
                return Exit.ERROR

    if config.aliases is None:
        aliases = None
    else:
        with config.aliases as data:
            try:
                aliases = json.load(data)
            except json.JSONDecodeError as e:
                print(f"Unable to load alias mapping: {e}", file=sys.stderr)  # noqa: T201
                return Exit.ERROR
        if not isinstance(aliases, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in aliases.items()
        ):
            print(  # noqa: T201
                'Alias mapping must be a JSON string mapping (e.g. {"from": "to", ...})',
                file=sys.stderr,
            )
            return Exit.ERROR

    if config.custom_formatters_kwargs is None:
        custom_formatters_kwargs = None
    else:
        with config.custom_formatters_kwargs as data:
            try:
                custom_formatters_kwargs = json.load(data)
            except json.JSONDecodeError as e:  # pragma: no cover
                print(  # noqa: T201
                    f"Unable to load custom_formatters_kwargs mapping: {e}",
                    file=sys.stderr,
                )
                return Exit.ERROR
        if not isinstance(custom_formatters_kwargs, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in custom_formatters_kwargs.items()
        ):  # pragma: no cover
            print(  # noqa: T201
                'Custom formatters kwargs mapping must be a JSON string mapping (e.g. {"from": "to", ...})',
                file=sys.stderr,
            )
            return Exit.ERROR

    try:
        generate(
            input_=config.url or config.input or sys.stdin.read(),
            input_file_type=config.input_file_type,
            output=config.output,
            output_model_type=config.output_model_type,
            target_python_version=config.target_python_version,
            base_class=config.base_class,
            additional_imports=config.additional_imports,
            custom_template_dir=config.custom_template_dir,
            validation=config.validation,
            field_constraints=config.field_constraints,
            snake_case_field=config.snake_case_field,
            strip_default_none=config.strip_default_none,
            extra_template_data=extra_template_data,
            aliases=aliases,
            disable_timestamp=config.disable_timestamp,
            enable_version_header=config.enable_version_header,
            allow_population_by_field_name=config.allow_population_by_field_name,
            allow_extra_fields=config.allow_extra_fields,
            extra_fields=config.extra_fields,
            apply_default_values_for_required_fields=config.use_default,
            force_optional_for_required_fields=config.force_optional,
            class_name=config.class_name,
            use_standard_collections=config.use_standard_collections,
            use_schema_description=config.use_schema_description,
            use_field_description=config.use_field_description,
            use_default_kwarg=config.use_default_kwarg,
            reuse_model=config.reuse_model,
            encoding=config.encoding,
            enum_field_as_literal=config.enum_field_as_literal,
            use_one_literal_as_default=config.use_one_literal_as_default,
            set_default_enum_member=config.set_default_enum_member,
            use_subclass_enum=config.use_subclass_enum,
            strict_nullable=config.strict_nullable,
            use_generic_container_types=config.use_generic_container_types,
            enable_faux_immutability=config.enable_faux_immutability,
            disable_appending_item_suffix=config.disable_appending_item_suffix,
            strict_types=config.strict_types,
            empty_enum_field_name=config.empty_enum_field_name,
            field_extra_keys=config.field_extra_keys,
            field_include_all_keys=config.field_include_all_keys,
            field_extra_keys_without_x_prefix=config.field_extra_keys_without_x_prefix,
            openapi_scopes=config.openapi_scopes,
            include_path_parameters=config.include_path_parameters,
            wrap_string_literal=config.wrap_string_literal,
            use_title_as_name=config.use_title_as_name,
            use_operation_id_as_name=config.use_operation_id_as_name,
            use_unique_items_as_set=config.use_unique_items_as_set,
            http_headers=config.http_headers,
            http_ignore_tls=config.http_ignore_tls,
            use_annotated=config.use_annotated,
            use_non_positive_negative_number_constrained_types=config.use_non_positive_negative_number_constrained_types,
            original_field_name_delimiter=config.original_field_name_delimiter,
            use_double_quotes=config.use_double_quotes,
            collapse_root_models=config.collapse_root_models,
            use_union_operator=config.use_union_operator,
            special_field_name_prefix=config.special_field_name_prefix,
            remove_special_field_name_prefix=config.remove_special_field_name_prefix,
            capitalise_enum_members=config.capitalise_enum_members,
            keep_model_order=config.keep_model_order,
            custom_file_header=config.custom_file_header,
            custom_file_header_path=config.custom_file_header_path,
            custom_formatters=config.custom_formatters,
            custom_formatters_kwargs=custom_formatters_kwargs,
            use_pendulum=config.use_pendulum,
            http_query_parameters=config.http_query_parameters,
            treat_dot_as_module=config.treat_dot_as_module,
            use_exact_imports=config.use_exact_imports,
            union_mode=config.union_mode,
            output_datetime_class=config.output_datetime_class,
            keyword_only=config.keyword_only,
            frozen_dataclasses=config.frozen_dataclasses,
            no_alias=config.no_alias,
            formatters=config.formatters,
            parent_scoped_naming=config.parent_scoped_naming,
            disable_future_imports=config.disable_future_imports,
        )
    except InvalidClassNameError as e:
        print(f"{e} You have to set `--class-name` option", file=sys.stderr)  # noqa: T201
        return Exit.ERROR
    except Error as e:
        print(str(e), file=sys.stderr)  # noqa: T201
        return Exit.ERROR
    except Exception:  # noqa: BLE001
        import traceback  # noqa: PLC0415

        print(traceback.format_exc(), file=sys.stderr)  # noqa: T201
        return Exit.ERROR
    else:
        return Exit.OK


if __name__ == "__main__":
    sys.exit(main())
