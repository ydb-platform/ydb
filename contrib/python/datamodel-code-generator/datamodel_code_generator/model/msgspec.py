from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar

from pydantic import Field

from datamodel_code_generator import DatetimeClassType, PythonVersion, PythonVersionMin
from datamodel_code_generator.imports import (
    IMPORT_DATE,
    IMPORT_DATETIME,
    IMPORT_TIME,
    IMPORT_TIMEDELTA,
    Import,
)
from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model.base import UNDEFINED
from datamodel_code_generator.model.imports import (
    IMPORT_CLASSVAR,
    IMPORT_MSGSPEC_CONVERT,
    IMPORT_MSGSPEC_FIELD,
    IMPORT_MSGSPEC_META,
)
from datamodel_code_generator.model.pydantic.base_model import (
    Constraints as _Constraints,
)
from datamodel_code_generator.model.rootmodel import RootModel as _RootModel
from datamodel_code_generator.model.types import DataTypeManager as _DataTypeManager
from datamodel_code_generator.model.types import type_map_factory
from datamodel_code_generator.types import (
    DataType,
    StrictTypes,
    Types,
    chain_as_tuple,
    get_optional_type,
)

if TYPE_CHECKING:
    from collections import defaultdict
    from collections.abc import Sequence
    from pathlib import Path

    from datamodel_code_generator.reference import Reference


def _has_field_assignment(field: DataModelFieldBase) -> bool:
    return not (field.required or (field.represented_default == "None" and field.strip_default_none))


DataModelFieldBaseT = TypeVar("DataModelFieldBaseT", bound=DataModelFieldBase)


def import_extender(cls: type[DataModelFieldBaseT]) -> type[DataModelFieldBaseT]:
    original_imports: property = cls.imports

    @wraps(original_imports.fget)  # pyright: ignore[reportArgumentType]
    def new_imports(self: DataModelFieldBaseT) -> tuple[Import, ...]:
        extra_imports = []
        field = self.field
        # TODO: Improve field detection
        if field and field.startswith("field("):
            extra_imports.append(IMPORT_MSGSPEC_FIELD)
        if self.field and "lambda: convert" in self.field:
            extra_imports.append(IMPORT_MSGSPEC_CONVERT)
        if self.annotated:
            extra_imports.append(IMPORT_MSGSPEC_META)
        if self.extras.get("is_classvar"):
            extra_imports.append(IMPORT_CLASSVAR)
        return chain_as_tuple(original_imports.fget(self), extra_imports)  # pyright: ignore[reportOptionalCall]

    cls.imports = property(new_imports)  # pyright: ignore[reportAttributeAccessIssue]
    return cls


class RootModel(_RootModel):
    pass


class Struct(DataModel):
    TEMPLATE_FILE_PATH: ClassVar[str] = "msgspec.jinja2"
    BASE_CLASS: ClassVar[str] = "msgspec.Struct"
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
        treat_dot_as_module: bool = False,
    ) -> None:
        super().__init__(
            reference=reference,
            fields=sorted(fields, key=_has_field_assignment),
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
        self.extra_template_data.setdefault("base_class_kwargs", {})
        if self.keyword_only:
            self.add_base_class_kwarg("kw_only", "True")

    def add_base_class_kwarg(self, name: str, value: str) -> None:
        self.extra_template_data["base_class_kwargs"][name] = value


class Constraints(_Constraints):
    # To override existing pattern alias
    regex: Optional[str] = Field(None, alias="regex")  # noqa: UP045
    pattern: Optional[str] = Field(None, alias="pattern")  # noqa: UP045


@import_extender
class DataModelField(DataModelFieldBase):
    _FIELD_KEYS: ClassVar[set[str]] = {
        "default",
        "default_factory",
    }
    _META_FIELD_KEYS: ClassVar[set[str]] = {
        "title",
        "description",
        "gt",
        "ge",
        "lt",
        "le",
        "multiple_of",
        # 'min_items', # not supported by msgspec
        # 'max_items', # not supported by msgspec
        "min_length",
        "max_length",
        "pattern",
        "examples",
        # 'unique_items', # not supported by msgspec
    }
    _PARSE_METHOD = "convert"
    _COMPARE_EXPRESSIONS: ClassVar[set[str]] = {"gt", "ge", "lt", "le", "multiple_of"}
    constraints: Optional[Constraints] = None  # noqa: UP045

    def self_reference(self) -> bool:  # pragma: no cover
        return isinstance(self.parent, Struct) and self.parent.reference.path in {
            d.reference.path for d in self.data_type.all_data_types if d.reference
        }

    def process_const(self) -> None:
        if "const" not in self.extras:
            return
        self.const = True
        self.nullable = False
        const = self.extras["const"]
        if self.data_type.type == "str" and isinstance(const, str):  # pragma: no cover # Literal supports only str
            self.data_type = self.data_type.__class__(literals=[const])

    def _get_strict_field_constraint_value(self, constraint: str, value: Any) -> Any:
        if value is None or constraint not in self._COMPARE_EXPRESSIONS:
            return value

        if any(data_type.type == "float" for data_type in self.data_type.all_data_types):
            return float(value)
        return int(value)

    @property
    def field(self) -> str | None:
        """for backwards compatibility"""
        result = str(self)
        if not result:
            return None
        return result

    def __str__(self) -> str:
        data: dict[str, Any] = {k: v for k, v in self.extras.items() if k in self._FIELD_KEYS}
        if self.alias:
            data["name"] = self.alias

        if self.default != UNDEFINED and self.default is not None:
            data["default"] = self.default
        elif not self.required:
            data["default"] = None

        if self.required:
            data = {
                k: v
                for k, v in data.items()
                if k
                not in {
                    "default",
                    "default_factory",
                }
            }
        elif self.default and "default_factory" not in data:
            default_factory = self._get_default_as_struct_model()
            if default_factory is not None:
                data.pop("default")
                data["default_factory"] = default_factory

        if not data:
            return ""

        if len(data) == 1 and "default" in data:
            return repr(data["default"])

        kwargs = [f"{k}={v if k == 'default_factory' else repr(v)}" for k, v in data.items()]
        return f"field({', '.join(kwargs)})"

    @property
    def annotated(self) -> str | None:
        if not self.use_annotated:  # pragma: no cover
            return None

        data: dict[str, Any] = {k: v for k, v in self.extras.items() if k in self._META_FIELD_KEYS}
        if self.constraints is not None and not self.self_reference() and not self.data_type.strict:
            data = {
                **data,
                **{
                    k: self._get_strict_field_constraint_value(k, v)
                    for k, v in self.constraints.dict().items()
                    if k in self._META_FIELD_KEYS
                },
            }

        meta_arguments = sorted(f"{k}={v!r}" for k, v in data.items() if v is not None)
        if not meta_arguments:
            return None

        meta = f"Meta({', '.join(meta_arguments)})"

        if not self.required and not self.extras.get("is_classvar"):
            type_hint = self.data_type.type_hint
            annotated_type = f"Annotated[{type_hint}, {meta}]"
            return get_optional_type(annotated_type, self.data_type.use_union_operator)

        annotated_type = f"Annotated[{self.type_hint}, {meta}]"
        if self.extras.get("is_classvar"):
            annotated_type = f"ClassVar[{annotated_type}]"

        return annotated_type

    def _get_default_as_struct_model(self) -> str | None:
        for data_type in self.data_type.data_types or (self.data_type,):
            # TODO: Check nested data_types
            if data_type.is_dict or self.data_type.is_union:
                # TODO: Parse Union and dict model for default
                continue  # pragma: no cover
            if data_type.is_list and len(data_type.data_types) == 1:
                data_type_child = data_type.data_types[0]
                if (  # pragma: no cover
                    data_type_child.reference
                    and (isinstance(data_type_child.reference.source, (Struct, RootModel)))
                    and isinstance(self.default, list)
                ):
                    return (
                        f"lambda: {self._PARSE_METHOD}({self.default!r},  "
                        f"type=list[{data_type_child.alias or data_type_child.reference.source.class_name}])"
                    )
            elif data_type.reference and isinstance(data_type.reference.source, Struct):
                return (
                    f"lambda: {self._PARSE_METHOD}({self.default!r},  "
                    f"type={data_type.alias or data_type.reference.source.class_name})"
                )
        return None


class DataTypeManager(_DataTypeManager):
    def __init__(  # noqa: PLR0913, PLR0917
        self,
        python_version: PythonVersion = PythonVersionMin,
        use_standard_collections: bool = False,  # noqa: FBT001, FBT002
        use_generic_container_types: bool = False,  # noqa: FBT001, FBT002
        strict_types: Sequence[StrictTypes] | None = None,
        use_non_positive_negative_number_constrained_types: bool = False,  # noqa: FBT001, FBT002
        use_union_operator: bool = False,  # noqa: FBT001, FBT002
        use_pendulum: bool = False,  # noqa: FBT001, FBT002
        target_datetime_class: DatetimeClassType | None = None,
        treat_dot_as_module: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        super().__init__(
            python_version,
            use_standard_collections,
            use_generic_container_types,
            strict_types,
            use_non_positive_negative_number_constrained_types,
            use_union_operator,
            use_pendulum,
            target_datetime_class,
            treat_dot_as_module,
        )

        datetime_map = (
            {
                Types.time: self.data_type.from_import(IMPORT_TIME),
                Types.date: self.data_type.from_import(IMPORT_DATE),
                Types.date_time: self.data_type.from_import(IMPORT_DATETIME),
                Types.timedelta: self.data_type.from_import(IMPORT_TIMEDELTA),
            }
            if target_datetime_class is DatetimeClassType.Datetime
            else {}
        )

        self.type_map: dict[Types, DataType] = {
            **type_map_factory(self.data_type),
            **datetime_map,
        }
