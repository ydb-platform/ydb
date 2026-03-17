from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any, ClassVar

from datamodel_code_generator.imports import IMPORT_TYPE_ALIAS, Import
from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model.base import UNDEFINED

if TYPE_CHECKING:
    from pathlib import Path

    from datamodel_code_generator.reference import Reference

_INT: str = "int"
_FLOAT: str = "float"
_BOOLEAN: str = "bool"
_STR: str = "str"

# default graphql scalar types
DEFAULT_GRAPHQL_SCALAR_TYPE = _STR

DEFAULT_GRAPHQL_SCALAR_TYPES: dict[str, str] = {
    "Boolean": _BOOLEAN,
    "String": _STR,
    "ID": _STR,
    "Int": _INT,
    "Float": _FLOAT,
}


class DataTypeScalar(DataModel):
    TEMPLATE_FILE_PATH: ClassVar[str] = "Scalar.jinja2"
    BASE_CLASS: ClassVar[str] = ""
    DEFAULT_IMPORTS: ClassVar[tuple[Import, ...]] = (IMPORT_TYPE_ALIAS,)

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
        extra_template_data = extra_template_data or defaultdict(dict)

        scalar_name = reference.name
        if scalar_name not in extra_template_data:
            extra_template_data[scalar_name] = defaultdict(dict)

        # py_type
        py_type = extra_template_data[scalar_name].get(
            "py_type",
            DEFAULT_GRAPHQL_SCALAR_TYPES.get(reference.name, DEFAULT_GRAPHQL_SCALAR_TYPE),
        )
        extra_template_data[scalar_name]["py_type"] = py_type

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
