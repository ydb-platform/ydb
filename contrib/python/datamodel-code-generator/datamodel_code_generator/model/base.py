from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from functools import cached_property, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar
from warnings import warn

from jinja2 import Environment, PackageLoader, Template
from pydantic import Field

from datamodel_code_generator.imports import (
    IMPORT_ANNOTATED,
    IMPORT_OPTIONAL,
    IMPORT_UNION,
    Import,
)
from datamodel_code_generator.reference import Reference, _BaseModel
from datamodel_code_generator.types import (
    ANY,
    NONE,
    UNION_PREFIX,
    DataType,
    Nullable,
    chain_as_tuple,
    get_optional_type,
)
from datamodel_code_generator.util import PYDANTIC_V2, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Iterator

TEMPLATE_DIR: Path = Path(__file__).parents[0] / "template"

ALL_MODEL: str = "#all#"

ConstraintsBaseT = TypeVar("ConstraintsBaseT", bound="ConstraintsBase")


class ConstraintsBase(_BaseModel):
    unique_items: Optional[bool] = Field(None, alias="uniqueItems")  # noqa: UP045
    _exclude_fields: ClassVar[set[str]] = {"has_constraints"}
    if PYDANTIC_V2:
        model_config = ConfigDict(  # pyright: ignore[reportAssignmentType]
            arbitrary_types_allowed=True, ignored_types=(cached_property,)
        )
    else:

        class Config:
            arbitrary_types_allowed = True
            keep_untouched = (cached_property,)

    @cached_property
    def has_constraints(self) -> bool:
        return any(v is not None for v in self.dict().values())

    @staticmethod
    def merge_constraints(a: ConstraintsBaseT, b: ConstraintsBaseT) -> ConstraintsBaseT | None:
        constraints_class = None
        if isinstance(a, ConstraintsBase):  # pragma: no cover
            root_type_field_constraints = {k: v for k, v in a.dict(by_alias=True).items() if v is not None}
            constraints_class = a.__class__
        else:
            root_type_field_constraints = {}  # pragma: no cover

        if isinstance(b, ConstraintsBase):  # pragma: no cover
            model_field_constraints = {k: v for k, v in b.dict(by_alias=True).items() if v is not None}
            constraints_class = constraints_class or b.__class__
        else:
            model_field_constraints = {}

        if constraints_class is None or not issubclass(constraints_class, ConstraintsBase):  # pragma: no cover
            return None

        return constraints_class.parse_obj({
            **root_type_field_constraints,
            **model_field_constraints,
        })


class DataModelFieldBase(_BaseModel):
    name: Optional[str] = None  # noqa: UP045
    default: Optional[Any] = None  # noqa: UP045
    required: bool = False
    alias: Optional[str] = None  # noqa: UP045
    data_type: DataType
    constraints: Any = None
    strip_default_none: bool = False
    nullable: Optional[bool] = None  # noqa: UP045
    parent: Optional[Any] = None  # noqa: UP045
    extras: dict[str, Any] = {}  # noqa: RUF012
    use_annotated: bool = False
    has_default: bool = False
    use_field_description: bool = False
    const: bool = False
    original_name: Optional[str] = None  # noqa: UP045
    use_default_kwarg: bool = False
    use_one_literal_as_default: bool = False
    _exclude_fields: ClassVar[set[str]] = {"parent"}
    _pass_fields: ClassVar[set[str]] = {"parent", "data_type"}
    can_have_extra_keys: ClassVar[bool] = True
    type_has_null: Optional[bool] = None  # noqa: UP045

    if not TYPE_CHECKING:

        def __init__(self, **data: Any) -> None:
            super().__init__(**data)
            if self.data_type.reference or self.data_type.data_types:
                self.data_type.parent = self
            self.process_const()

    def process_const(self) -> None:
        if "const" not in self.extras:
            return
        self.default = self.extras["const"]
        self.const = True
        self.required = False
        self.nullable = False

    @property
    def type_hint(self) -> str:  # noqa: PLR0911
        type_hint = self.data_type.type_hint

        if not type_hint:
            return NONE
        if self.has_default_factory or (self.data_type.is_optional and self.data_type.type != ANY):
            return type_hint
        if self.nullable is not None:
            if self.nullable:
                return get_optional_type(type_hint, self.data_type.use_union_operator)
            return type_hint
        if self.required:
            if self.type_has_null:
                return get_optional_type(type_hint, self.data_type.use_union_operator)
            return type_hint
        if self.fall_back_to_nullable:
            return get_optional_type(type_hint, self.data_type.use_union_operator)
        return type_hint

    @property
    def imports(self) -> tuple[Import, ...]:
        type_hint = self.type_hint
        has_union = not self.data_type.use_union_operator and UNION_PREFIX in type_hint
        imports: list[tuple[Import] | Iterator[Import]] = [
            iter(i for i in self.data_type.all_imports if not (not has_union and i == IMPORT_UNION))
        ]

        if self.fall_back_to_nullable:
            if (
                self.nullable or (self.nullable is None and not self.required)
            ) and not self.data_type.use_union_operator:
                imports.append((IMPORT_OPTIONAL,))
        elif self.nullable and not self.data_type.use_union_operator:  # pragma: no cover
            imports.append((IMPORT_OPTIONAL,))
        if self.use_annotated and self.annotated:
            imports.append((IMPORT_ANNOTATED,))
        return chain_as_tuple(*imports)

    @property
    def docstring(self) -> str | None:
        if self.use_field_description:
            description = self.extras.get("description", None)
            if description is not None:
                return f"{description}"
        return None

    @property
    def unresolved_types(self) -> frozenset[str]:
        return self.data_type.unresolved_types

    @property
    def field(self) -> str | None:
        """for backwards compatibility"""
        return None

    @property
    def method(self) -> str | None:
        return None

    @property
    def represented_default(self) -> str:
        return repr(self.default)

    @property
    def annotated(self) -> str | None:
        return None

    @property
    def has_default_factory(self) -> bool:
        return "default_factory" in self.extras

    @property
    def fall_back_to_nullable(self) -> bool:
        return True


@lru_cache
def get_template(template_file_path: Path) -> Template:
    template_dir = Path("template") / template_file_path.parent
    loader = PackageLoader(__package__, template_dir)
    environment: Environment = Environment(loader=loader)  # noqa: S701
    return environment.get_template(template_file_path.name)


def sanitize_module_name(name: str, *, treat_dot_as_module: bool) -> str:
    pattern = r"[^0-9a-zA-Z_.]" if treat_dot_as_module else r"[^0-9a-zA-Z_]"
    sanitized = re.sub(pattern, "_", name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def get_module_path(name: str, file_path: Path | None, *, treat_dot_as_module: bool) -> list[str]:
    if file_path:
        sanitized_stem = sanitize_module_name(file_path.stem, treat_dot_as_module=treat_dot_as_module)
        return [
            *file_path.parts[:-1],
            sanitized_stem,
            *name.split(".")[:-1],
        ]
    return name.split(".")[:-1]


def get_module_name(name: str, file_path: Path | None, *, treat_dot_as_module: bool) -> str:
    return ".".join(get_module_path(name, file_path, treat_dot_as_module=treat_dot_as_module))


class TemplateBase(ABC):
    @cached_property
    @abstractmethod
    def template_file_path(self) -> Path:
        raise NotImplementedError

    @cached_property
    def template(self) -> Template:
        return get_template(self.template_file_path)

    @abstractmethod
    def render(self) -> str:
        raise NotImplementedError

    def _render(self, *args: Any, **kwargs: Any) -> str:
        return self.template.render(*args, **kwargs)

    def __str__(self) -> str:
        return self.render()


class BaseClassDataType(DataType): ...


UNDEFINED: Any = object()


class DataModel(TemplateBase, Nullable, ABC):
    TEMPLATE_FILE_PATH: ClassVar[str] = ""
    BASE_CLASS: ClassVar[str] = ""
    DEFAULT_IMPORTS: ClassVar[tuple[Import, ...]] = ()

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
        frozen: bool = False,
        treat_dot_as_module: bool = False,
    ) -> None:
        self.keyword_only = keyword_only
        self.frozen = frozen
        if not self.TEMPLATE_FILE_PATH:
            msg = "TEMPLATE_FILE_PATH is undefined"
            raise Exception(msg)  # noqa: TRY002

        self._custom_template_dir: Path | None = custom_template_dir
        self.decorators: list[str] = decorators or []
        self._additional_imports: list[Import] = []
        self.custom_base_class = custom_base_class
        if base_classes:
            self.base_classes: list[BaseClassDataType] = [BaseClassDataType(reference=b) for b in base_classes]
        else:
            self.set_base_class()

        self.file_path: Path | None = path
        self.reference: Reference = reference

        self.reference.source = self

        if extra_template_data is not None:
            # The supplied defaultdict will either create a new entry,
            # or already contain a predefined entry for this type
            self.extra_template_data = extra_template_data[self.reference.path]

            # We use the full object reference path as dictionary key, but
            # we still support `name` as key because it was used for
            # `--extra-template-data` input file and we don't want to break the
            # existing behavior.
            self.extra_template_data.update(extra_template_data[self.name])
        else:
            self.extra_template_data = defaultdict(dict)

        self.fields = self._validate_fields(fields) if fields else []

        for base_class in self.base_classes:
            if base_class.reference:
                base_class.reference.children.append(self)

        if extra_template_data is not None:
            all_model_extra_template_data = extra_template_data.get(ALL_MODEL)
            if all_model_extra_template_data:
                # The deepcopy is needed here to ensure that different models don't
                # end up inadvertently sharing state (such as "base_class_kwargs")
                self.extra_template_data.update(deepcopy(all_model_extra_template_data))

        self.methods: list[str] = methods or []

        self.description = description
        for field in self.fields:
            field.parent = self

        self._additional_imports.extend(self.DEFAULT_IMPORTS)
        self.default: Any = default
        self._nullable: bool = nullable
        self._treat_dot_as_module: bool = treat_dot_as_module

    def _validate_fields(self, fields: list[DataModelFieldBase]) -> list[DataModelFieldBase]:
        names: set[str] = set()
        unique_fields: list[DataModelFieldBase] = []
        for field in fields:
            if field.name:
                if field.name in names:
                    warn(f"Field name `{field.name}` is duplicated on {self.name}", stacklevel=2)
                    continue
                names.add(field.name)
            unique_fields.append(field)
        return unique_fields

    def set_base_class(self) -> None:
        base_class = self.custom_base_class or self.BASE_CLASS
        if not base_class:
            self.base_classes = []
            return
        base_class_import = Import.from_full_path(base_class)
        self._additional_imports.append(base_class_import)
        self.base_classes = [BaseClassDataType.from_import(base_class_import)]

    @cached_property
    def template_file_path(self) -> Path:
        template_file_path = Path(self.TEMPLATE_FILE_PATH)
        if self._custom_template_dir is not None:
            custom_template_file_path = self._custom_template_dir / template_file_path
            if custom_template_file_path.exists():
                return custom_template_file_path
        return template_file_path

    @property
    def imports(self) -> tuple[Import, ...]:
        return chain_as_tuple(
            (i for f in self.fields for i in f.imports),
            self._additional_imports,
        )

    @property
    def reference_classes(self) -> frozenset[str]:
        return frozenset(
            {r.reference.path for r in self.base_classes if r.reference}
            | {t for f in self.fields for t in f.unresolved_types}
        )

    @property
    def name(self) -> str:
        return self.reference.name

    @property
    def duplicate_name(self) -> str:
        return self.reference.duplicate_name or ""

    @property
    def base_class(self) -> str:
        return ", ".join(b.type_hint for b in self.base_classes)

    @staticmethod
    def _get_class_name(name: str) -> str:
        if "." in name:
            return name.rsplit(".", 1)[-1]
        return name

    @property
    def class_name(self) -> str:
        return self._get_class_name(self.name)

    @class_name.setter
    def class_name(self, class_name: str) -> None:
        if "." in self.reference.name:
            self.reference.name = f"{self.reference.name.rsplit('.', 1)[0]}.{class_name}"
        else:
            self.reference.name = class_name

    @property
    def duplicate_class_name(self) -> str:
        return self._get_class_name(self.duplicate_name)

    @property
    def module_path(self) -> list[str]:
        return get_module_path(self.name, self.file_path, treat_dot_as_module=self._treat_dot_as_module)

    @property
    def module_name(self) -> str:
        return get_module_name(self.name, self.file_path, treat_dot_as_module=self._treat_dot_as_module)

    @property
    def all_data_types(self) -> Iterator[DataType]:
        for field in self.fields:
            yield from field.data_type.all_data_types
        yield from self.base_classes

    @property
    def nullable(self) -> bool:
        return self._nullable

    @cached_property
    def path(self) -> str:
        return self.reference.path

    def render(self, *, class_name: str | None = None) -> str:
        return self._render(
            class_name=class_name or self.class_name,
            fields=self.fields,
            decorators=self.decorators,
            base_class=self.base_class,
            methods=self.methods,
            description=self.description,
            keyword_only=self.keyword_only,
            frozen=self.frozen,
            **self.extra_template_data,
        )
