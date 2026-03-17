from dataclasses import dataclass
from enum import Enum as PyEnum
from typing import Tuple, Dict, FrozenSet, Optional

from sqlalchemy import Enum, ARRAY


class ColumnType(PyEnum):
    COMMON = Enum
    ARRAY = ARRAY

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"


Unspecified = object()


@dataclass(frozen=True)
class TableReference:
    table_name: str
    column_name: str
    table_schema: Optional[str] = Unspecified  # type: ignore[assignment] # 'Unspecified' default is for migrations from older versions
    column_type: ColumnType = ColumnType.COMMON
    existing_server_default: Optional[str] = None

    def __repr__(self):
        result_str = "TableReference("
        if self.table_schema:
            result_str += f"table_schema={self.table_schema!r}, "
        result_str += f"table_name={self.table_name!r}, "
        result_str += f"column_name={self.column_name!r}, "
        if self.column_type != ColumnType.COMMON:
            result_str += f"column_type=ColumnType.{self.column_type.name}, "
        if self.existing_server_default is not None:
            result_str += f"existing_server_default={self.existing_server_default!r}, "
        result_str = result_str[:-2]  # to remove last comma with space
        result_str += ")"
        return result_str

    @property
    def is_column_type_import_needed(self):
        return self.column_type != ColumnType.COMMON

    @property
    def table_name_with_schema(self):
        if self.table_schema:
            prefix = f'"{self.table_schema}".'
        else:
            prefix = ""
        return f'{prefix}"{self.table_name}"'

    @property
    def escaped_column_name(self):
        return f'"{self.column_name}"'


EnumNamesToValues = Dict[str, Tuple[str, ...]]
EnumNamesToTableReferences = Dict[str, FrozenSet[TableReference]]


@dataclass
class DeclaredEnumValues:
    enum_values: EnumNamesToValues
    enum_table_references: EnumNamesToTableReferences
