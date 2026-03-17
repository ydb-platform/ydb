from __future__ import annotations

import keyword
from typing import TYPE_CHECKING, Any, ClassVar

from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model.base import UNDEFINED
from datamodel_code_generator.model.imports import (
    IMPORT_NOT_REQUIRED,
    IMPORT_NOT_REQUIRED_BACKPORT,
    IMPORT_TYPED_DICT,
)
from datamodel_code_generator.types import NOT_REQUIRED_PREFIX

if TYPE_CHECKING:
    from collections import defaultdict
    from collections.abc import Iterator
    from pathlib import Path

    from datamodel_code_generator.reference import Reference

from datamodel_code_generator.imports import Import  # noqa: TC001

escape_characters = str.maketrans({
    "\\": r"\\",
    "'": r"\'",
    "\b": r"\b",
    "\f": r"\f",
    "\n": r"\n",
    "\r": r"\r",
    "\t": r"\t",
})


def _is_valid_field_name(field: DataModelFieldBase) -> bool:
    name = field.original_name or field.name
    if name is None:  # pragma: no cover
        return False
    return name.isidentifier() and not keyword.iskeyword(name)


class TypedDict(DataModel):
    TEMPLATE_FILE_PATH: ClassVar[str] = "TypedDict.jinja2"
    BASE_CLASS: ClassVar[str] = "typing.TypedDict"
    DEFAULT_IMPORTS: ClassVar[tuple[Import, ...]] = (IMPORT_TYPED_DICT,)

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

    @property
    def is_functional_syntax(self) -> bool:
        return any(not _is_valid_field_name(f) for f in self.fields)

    @property
    def all_fields(self) -> Iterator[DataModelFieldBase]:
        for base_class in self.base_classes:
            if base_class.reference is None:  # pragma: no cover
                continue
            data_model = base_class.reference.source
            if not isinstance(data_model, DataModel):  # pragma: no cover
                continue

            if isinstance(data_model, TypedDict):  # pragma: no cover
                yield from data_model.all_fields

        yield from self.fields

    def render(self, *, class_name: str | None = None) -> str:
        return self._render(
            class_name=class_name or self.class_name,
            fields=self.fields,
            decorators=self.decorators,
            base_class=self.base_class,
            methods=self.methods,
            description=self.description,
            is_functional_syntax=self.is_functional_syntax,
            all_fields=self.all_fields,
            **self.extra_template_data,
        )


class DataModelField(DataModelFieldBase):
    DEFAULT_IMPORTS: ClassVar[tuple[Import, ...]] = (IMPORT_NOT_REQUIRED,)

    def process_const(self) -> None:
        if "const" not in self.extras:
            return
        self.const = True
        self.nullable = False
        const = self.extras["const"]
        self.data_type = self.data_type.__class__(literals=[const])
        if not self.default:
            self.default = const

    @property
    def key(self) -> str:
        return (self.original_name or self.name or "").translate(  # pragma: no cover
            escape_characters
        )

    @property
    def type_hint(self) -> str:
        type_hint = super().type_hint
        if self._not_required:
            return f"{NOT_REQUIRED_PREFIX}{type_hint}]"
        return type_hint

    @property
    def _not_required(self) -> bool:
        return not self.required and isinstance(self.parent, TypedDict)

    @property
    def fall_back_to_nullable(self) -> bool:
        return not self._not_required

    @property
    def imports(self) -> tuple[Import, ...]:
        return (
            *super().imports,
            *(self.DEFAULT_IMPORTS if self._not_required else ()),
        )


class DataModelFieldBackport(DataModelField):
    DEFAULT_IMPORTS: ClassVar[tuple[Import, ...]] = (IMPORT_NOT_REQUIRED_BACKPORT,)
