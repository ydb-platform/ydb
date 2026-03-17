from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Optional

from datamodel_code_generator.imports import IMPORT_ANY, IMPORT_ENUM, Import
from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model.base import UNDEFINED, BaseClassDataType
from datamodel_code_generator.types import DataType, Types

if TYPE_CHECKING:
    from collections import defaultdict
    from pathlib import Path

    from datamodel_code_generator.reference import Reference

_INT: str = "int"
_FLOAT: str = "float"
_BYTES: str = "bytes"
_STR: str = "str"

SUBCLASS_BASE_CLASSES: dict[Types, str] = {
    Types.int32: _INT,
    Types.int64: _INT,
    Types.integer: _INT,
    Types.float: _FLOAT,
    Types.double: _FLOAT,
    Types.number: _FLOAT,
    Types.byte: _BYTES,
    Types.string: _STR,
}


class Enum(DataModel):
    TEMPLATE_FILE_PATH: ClassVar[str] = "Enum.jinja2"
    BASE_CLASS: ClassVar[str] = "enum.Enum"
    DEFAULT_IMPORTS: ClassVar[tuple[Import, ...]] = (IMPORT_ENUM,)

    def __init__(  # noqa: PLR0913
        self,
        *,
        reference: Reference,
        fields: list[DataModelFieldBase],
        decorators: list[str] | None = None,
        base_classes: list[Reference] | None = None,
        custom_base_class: str | None = None,
        custom_template_dir: Path | None = None,
        extra_template_data: defaultdict[str, dict[str, Any]] | None = None,
        methods: list[str] | None = None,
        path: Path | None = None,
        description: str | None = None,
        type_: Types | None = None,
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
            methods=methods,
            path=path,
            description=description,
            default=default,
            nullable=nullable,
            keyword_only=keyword_only,
            treat_dot_as_module=treat_dot_as_module,
        )

        if not base_classes and type_:
            base_class = SUBCLASS_BASE_CLASSES.get(type_)
            if base_class:
                self.base_classes: list[BaseClassDataType] = [
                    BaseClassDataType(type=base_class),
                    *self.base_classes,
                ]

    @classmethod
    def get_data_type(cls, types: Types, **kwargs: Any) -> DataType:
        raise NotImplementedError

    def get_member(self, field: DataModelFieldBase) -> Member:
        return Member(self, field)

    def find_member(self, value: Any) -> Member | None:
        repr_value = repr(value)
        # Remove surrounding quotes from the string representation
        str_value = str(value).strip("'\"")

        for field in self.fields:
            # Remove surrounding quotes from field default value
            field_default = str(field.default or "").strip("'\"")

            # Compare values after removing quotes
            if field_default == str_value:
                return self.get_member(field)

            # Keep original comparison for backwards compatibility
            if field.default == repr_value:  # pragma: no cover
                return self.get_member(field)

        return None

    @property
    def imports(self) -> tuple[Import, ...]:
        return tuple(i for i in super().imports if i != IMPORT_ANY)


class Member:
    def __init__(self, enum: Enum, field: DataModelFieldBase) -> None:
        self.enum: Enum = enum
        self.field: DataModelFieldBase = field
        self.alias: Optional[str] = None  # noqa: UP045

    def __repr__(self) -> str:
        return f"{self.alias or self.enum.name}.{self.field.name}"
