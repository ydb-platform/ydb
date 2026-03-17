from __future__ import annotations

from abc import ABC
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import Field

from datamodel_code_generator.model import (
    ConstraintsBase,
    DataModel,
    DataModelFieldBase,
)
from datamodel_code_generator.model.base import UNDEFINED
from datamodel_code_generator.model.pydantic.imports import (
    IMPORT_ANYURL,
    IMPORT_EXTRA,
    IMPORT_FIELD,
)
from datamodel_code_generator.types import UnionIntFloat, chain_as_tuple

if TYPE_CHECKING:
    from collections import defaultdict

    from datamodel_code_generator.imports import Import
    from datamodel_code_generator.reference import Reference


class Constraints(ConstraintsBase):
    gt: Optional[UnionIntFloat] = Field(None, alias="exclusiveMinimum")  # noqa: UP045
    ge: Optional[UnionIntFloat] = Field(None, alias="minimum")  # noqa: UP045
    lt: Optional[UnionIntFloat] = Field(None, alias="exclusiveMaximum")  # noqa: UP045
    le: Optional[UnionIntFloat] = Field(None, alias="maximum")  # noqa: UP045
    multiple_of: Optional[float] = Field(None, alias="multipleOf")  # noqa: UP045
    min_items: Optional[int] = Field(None, alias="minItems")  # noqa: UP045
    max_items: Optional[int] = Field(None, alias="maxItems")  # noqa: UP045
    min_length: Optional[int] = Field(None, alias="minLength")  # noqa: UP045
    max_length: Optional[int] = Field(None, alias="maxLength")  # noqa: UP045
    regex: Optional[str] = Field(None, alias="pattern")  # noqa: UP045


class DataModelField(DataModelFieldBase):
    _EXCLUDE_FIELD_KEYS: ClassVar[set[str]] = {
        "alias",
        "default",
        "const",
        "gt",
        "ge",
        "lt",
        "le",
        "multiple_of",
        "min_items",
        "max_items",
        "min_length",
        "max_length",
        "regex",
    }
    _COMPARE_EXPRESSIONS: ClassVar[set[str]] = {"gt", "ge", "lt", "le"}
    constraints: Optional[Constraints] = None  # noqa: UP045
    _PARSE_METHOD: ClassVar[str] = "parse_obj"

    @property
    def method(self) -> str | None:
        return self.validator

    @property
    def validator(self) -> str | None:
        return None
        # TODO refactor this method for other validation logic

    @property
    def field(self) -> str | None:
        """for backwards compatibility"""
        result = str(self)
        if (
            self.use_default_kwarg
            and not result.startswith("Field(...")
            and not result.startswith("Field(default_factory=")
        ):
            # Use `default=` for fields that have a default value so that type
            # checkers using @dataclass_transform can infer the field as
            # optional in __init__.
            result = result.replace("Field(", "Field(default=")
        if not result:
            return None
        return result

    def self_reference(self) -> bool:
        return isinstance(self.parent, BaseModelBase) and self.parent.reference.path in {
            d.reference.path for d in self.data_type.all_data_types if d.reference
        }

    def _get_strict_field_constraint_value(self, constraint: str, value: Any) -> Any:
        if value is None or constraint not in self._COMPARE_EXPRESSIONS:
            return value

        if any(data_type.type == "float" for data_type in self.data_type.all_data_types):
            return float(value)
        return int(value)

    def _get_default_as_pydantic_model(self) -> str | None:
        for data_type in self.data_type.data_types or (self.data_type,):
            # TODO: Check nested data_types
            if data_type.is_dict or self.data_type.is_union:
                # TODO: Parse Union and dict model for default
                continue
            if data_type.is_list and len(data_type.data_types) == 1:
                data_type_child = data_type.data_types[0]
                if (
                    data_type_child.reference
                    and isinstance(data_type_child.reference.source, BaseModelBase)
                    and isinstance(self.default, list)
                ):  # pragma: no cover
                    return (
                        f"lambda :[{data_type_child.alias or data_type_child.reference.source.class_name}."
                        f"{self._PARSE_METHOD}(v) for v in {self.default!r}]"
                    )
            elif data_type.reference and isinstance(data_type.reference.source, BaseModelBase):  # pragma: no cover
                return (
                    f"lambda :{data_type.alias or data_type.reference.source.class_name}."
                    f"{self._PARSE_METHOD}({self.default!r})"
                )
        return None

    def _process_data_in_str(self, data: dict[str, Any]) -> None:
        if self.const:
            data["const"] = True

    def _process_annotated_field_arguments(self, field_arguments: list[str]) -> list[str]:  # noqa: PLR6301
        return field_arguments

    def __str__(self) -> str:  # noqa: PLR0912
        data: dict[str, Any] = {k: v for k, v in self.extras.items() if k not in self._EXCLUDE_FIELD_KEYS}
        if self.alias:
            data["alias"] = self.alias
        if self.constraints is not None and not self.self_reference() and not self.data_type.strict:
            data = {
                **data,
                **(
                    {}
                    if any(d.import_ == IMPORT_ANYURL for d in self.data_type.all_data_types)
                    else {
                        k: self._get_strict_field_constraint_value(k, v)
                        for k, v in self.constraints.dict(exclude_unset=True).items()
                    }
                ),
            }

        if self.use_field_description:
            data.pop("description", None)  # Description is part of field docstring

        self._process_data_in_str(data)

        discriminator = data.pop("discriminator", None)
        if discriminator:
            if isinstance(discriminator, str):
                data["discriminator"] = discriminator
            elif isinstance(discriminator, dict):  # pragma: no cover
                data["discriminator"] = discriminator["propertyName"]

        if self.required:
            default_factory = None
        elif self.default and "default_factory" not in data:
            default_factory = self._get_default_as_pydantic_model()
        else:
            default_factory = data.pop("default_factory", None)

        field_arguments = sorted(f"{k}={v!r}" for k, v in data.items() if v is not None)

        if not field_arguments and not default_factory:
            if self.nullable and self.required:
                return "Field(...)"  # Field() is for mypy
            return ""

        if self.use_annotated:
            field_arguments = self._process_annotated_field_arguments(field_arguments)
        elif self.required:
            field_arguments = ["...", *field_arguments]
        elif default_factory:
            field_arguments = [f"default_factory={default_factory}", *field_arguments]
        else:
            field_arguments = [f"{self.default!r}", *field_arguments]

        return f"Field({', '.join(field_arguments)})"

    @property
    def annotated(self) -> str | None:
        if not self.use_annotated or not str(self):
            return None
        return f"Annotated[{self.type_hint}, {self!s}]"

    @property
    def imports(self) -> tuple[Import, ...]:
        if self.field:
            return chain_as_tuple(super().imports, (IMPORT_FIELD,))
        return super().imports


class BaseModelBase(DataModel, ABC):
    def __init__(  # noqa: PLR0913
        self,
        *,
        reference: Reference,
        fields: list[DataModelFieldBase],
        decorators: list[str] | None = None,
        base_classes: list[Reference] | None = None,
        custom_base_class: str | None = None,
        custom_template_dir: Path | None = None,
        extra_template_data: defaultdict[str, Any] | None = None,
        path: Path | None = None,
        description: str | None = None,
        default: Any = UNDEFINED,
        nullable: bool = False,
        keyword_only: bool = False,
        treat_dot_as_module: bool = False,
    ) -> None:
        methods: list[str] = [field.method for field in fields if field.method]

        super().__init__(
            fields=fields,
            reference=reference,
            decorators=decorators,
            base_classes=base_classes,
            custom_base_class=custom_base_class,
            custom_template_dir=custom_template_dir,
            extra_template_data=extra_template_data,
            methods=methods,
            path=path,
            description=description,
            default=default,
            nullable=nullable,
            keyword_only=keyword_only,
            treat_dot_as_module=treat_dot_as_module,
        )

    @cached_property
    def template_file_path(self) -> Path:
        # This property is for Backward compatibility
        # Current version supports '{custom_template_dir}/BaseModel.jinja'
        # But, Future version will support only '{custom_template_dir}/pydantic/BaseModel.jinja'
        if self._custom_template_dir is not None:
            custom_template_file_path = self._custom_template_dir / Path(self.TEMPLATE_FILE_PATH).name
            if custom_template_file_path.exists():
                return custom_template_file_path
        return super().template_file_path


class BaseModel(BaseModelBase):
    TEMPLATE_FILE_PATH: ClassVar[str] = "pydantic/BaseModel.jinja2"
    BASE_CLASS: ClassVar[str] = "pydantic.BaseModel"

    def __init__(  # noqa: PLR0913
        self,
        *,
        reference: Reference,
        fields: list[DataModelFieldBase],
        decorators: list[str] | None = None,
        base_classes: list[Reference] | None = None,
        custom_base_class: str | None = None,
        custom_template_dir: Path | None = None,
        extra_template_data: defaultdict[str, Any] | None = None,
        path: Path | None = None,
        description: str | None = None,
        default: Any = UNDEFINED,
        nullable: bool = False,
        keyword_only: bool = False,
        treat_dot_as_module: bool = False,
    ) -> None:
        super().__init__(
            reference=reference,
            fields=fields,
            decorators=decorators,
            base_classes=base_classes,
            custom_base_class=custom_base_class,
            custom_template_dir=custom_template_dir,
            extra_template_data=extra_template_data,
            path=path,
            description=description,
            default=default,
            nullable=nullable,
            keyword_only=keyword_only,
            treat_dot_as_module=treat_dot_as_module,
        )
        config_parameters: dict[str, Any] = {}

        additional_properties = self.extra_template_data.get("additionalProperties")
        allow_extra_fields = self.extra_template_data.get("allow_extra_fields")
        extra_fields = self.extra_template_data.get("extra_fields")

        if allow_extra_fields or extra_fields or additional_properties is not None:
            self._additional_imports.append(IMPORT_EXTRA)

        if allow_extra_fields:
            config_parameters["extra"] = "Extra.allow"
        elif extra_fields:
            config_parameters["extra"] = f"Extra.{extra_fields}"
        elif additional_properties is True:
            config_parameters["extra"] = "Extra.allow"
        elif additional_properties is False:
            config_parameters["extra"] = "Extra.forbid"

        for config_attribute in "allow_population_by_field_name", "allow_mutation":
            if config_attribute in self.extra_template_data:
                config_parameters[config_attribute] = self.extra_template_data[config_attribute]
        for data_type in self.all_data_types:
            if data_type.is_custom_type:
                config_parameters["arbitrary_types_allowed"] = True
                break

        if isinstance(self.extra_template_data.get("config"), dict):
            for key, value in self.extra_template_data["config"].items():
                config_parameters[key] = value  # noqa: PERF403

        if config_parameters:
            from datamodel_code_generator.model.pydantic import Config  # noqa: PLC0415

            self.extra_template_data["config"] = Config.parse_obj(config_parameters)  # pyright: ignore[reportArgumentType]
