from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datamodel_code_generator import DatetimeClassType, PythonVersion, PythonVersionMin
from datamodel_code_generator.imports import (
    IMPORT_ANY,
    IMPORT_DECIMAL,
    IMPORT_TIMEDELTA,
)
from datamodel_code_generator.types import DataType, StrictTypes, Types
from datamodel_code_generator.types import DataTypeManager as _DataTypeManager

if TYPE_CHECKING:
    from collections.abc import Sequence


def type_map_factory(data_type: type[DataType]) -> dict[Types, DataType]:
    data_type_int = data_type(type="int")
    data_type_float = data_type(type="float")
    data_type_str = data_type(type="str")
    return {
        # TODO: Should we support a special type such UUID?
        Types.integer: data_type_int,
        Types.int32: data_type_int,
        Types.int64: data_type_int,
        Types.number: data_type_float,
        Types.float: data_type_float,
        Types.double: data_type_float,
        Types.decimal: data_type.from_import(IMPORT_DECIMAL),
        Types.time: data_type_str,
        Types.string: data_type_str,
        Types.byte: data_type_str,  # base64 encoded string
        Types.binary: data_type(type="bytes"),
        Types.date: data_type_str,
        Types.date_time: data_type_str,
        Types.timedelta: data_type.from_import(IMPORT_TIMEDELTA),
        Types.password: data_type_str,
        Types.email: data_type_str,
        Types.uuid: data_type_str,
        Types.uuid1: data_type_str,
        Types.uuid2: data_type_str,
        Types.uuid3: data_type_str,
        Types.uuid4: data_type_str,
        Types.uuid5: data_type_str,
        Types.uri: data_type_str,
        Types.hostname: data_type_str,
        Types.ipv4: data_type_str,
        Types.ipv6: data_type_str,
        Types.ipv4_network: data_type_str,
        Types.ipv6_network: data_type_str,
        Types.boolean: data_type(type="bool"),
        Types.object: data_type.from_import(IMPORT_ANY, is_dict=True),
        Types.null: data_type(type="None"),
        Types.array: data_type.from_import(IMPORT_ANY, is_list=True),
        Types.any: data_type.from_import(IMPORT_ANY),
    }


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

        self.type_map: dict[Types, DataType] = type_map_factory(self.data_type)

    def get_data_type(
        self,
        types: Types,
        **_: Any,
    ) -> DataType:
        return self.type_map[types]
