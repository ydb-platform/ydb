from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel as _BaseModel

from .base_model import BaseModel, DataModelField
from .custom_root_type import CustomRootType
from .dataclass import DataClass
from .types import DataTypeManager

if TYPE_CHECKING:
    from collections.abc import Iterable


def dump_resolve_reference_action(class_names: Iterable[str]) -> str:
    return "\n".join(f"{class_name}.update_forward_refs()" for class_name in class_names)


class Config(_BaseModel):
    extra: Optional[str] = None  # noqa: UP045
    title: Optional[str] = None  # noqa: UP045
    allow_population_by_field_name: Optional[bool] = None  # noqa: UP045
    allow_extra_fields: Optional[bool] = None  # noqa: UP045
    extra_fields: Optional[str] = None  # noqa: UP045
    allow_mutation: Optional[bool] = None  # noqa: UP045
    arbitrary_types_allowed: Optional[bool] = None  # noqa: UP045
    orm_mode: Optional[bool] = None  # noqa: UP045


__all__ = [
    "BaseModel",
    "CustomRootType",
    "DataClass",
    "DataModelField",
    "DataTypeManager",
    "dump_resolve_reference_action",
]
