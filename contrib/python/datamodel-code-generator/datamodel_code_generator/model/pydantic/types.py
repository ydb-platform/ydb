from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from datamodel_code_generator.format import DatetimeClassType, PythonVersion, PythonVersionMin
from datamodel_code_generator.imports import (
    IMPORT_ANY,
    IMPORT_DATE,
    IMPORT_DATETIME,
    IMPORT_DECIMAL,
    IMPORT_PATH,
    IMPORT_PENDULUM_DATE,
    IMPORT_PENDULUM_DATETIME,
    IMPORT_PENDULUM_DURATION,
    IMPORT_PENDULUM_TIME,
    IMPORT_TIME,
    IMPORT_TIMEDELTA,
    IMPORT_UUID,
)
from datamodel_code_generator.model.pydantic.imports import (
    IMPORT_ANYURL,
    IMPORT_CONBYTES,
    IMPORT_CONDECIMAL,
    IMPORT_CONFLOAT,
    IMPORT_CONINT,
    IMPORT_CONSTR,
    IMPORT_EMAIL_STR,
    IMPORT_IPV4ADDRESS,
    IMPORT_IPV4NETWORKS,
    IMPORT_IPV6ADDRESS,
    IMPORT_IPV6NETWORKS,
    IMPORT_NEGATIVE_FLOAT,
    IMPORT_NEGATIVE_INT,
    IMPORT_NON_NEGATIVE_FLOAT,
    IMPORT_NON_NEGATIVE_INT,
    IMPORT_NON_POSITIVE_FLOAT,
    IMPORT_NON_POSITIVE_INT,
    IMPORT_POSITIVE_FLOAT,
    IMPORT_POSITIVE_INT,
    IMPORT_SECRET_STR,
    IMPORT_STRICT_BOOL,
    IMPORT_STRICT_BYTES,
    IMPORT_STRICT_FLOAT,
    IMPORT_STRICT_INT,
    IMPORT_STRICT_STR,
    IMPORT_UUID1,
    IMPORT_UUID2,
    IMPORT_UUID3,
    IMPORT_UUID4,
    IMPORT_UUID5,
)
from datamodel_code_generator.types import DataType, StrictTypes, Types, UnionIntFloat
from datamodel_code_generator.types import DataTypeManager as _DataTypeManager

if TYPE_CHECKING:
    from collections.abc import Sequence


def type_map_factory(
    data_type: type[DataType],
    strict_types: Sequence[StrictTypes],
    pattern_key: str,
    use_pendulum: bool,  # noqa: FBT001
) -> dict[Types, DataType]:
    data_type_int = data_type(type="int")
    data_type_float = data_type(type="float")
    data_type_str = data_type(type="str")
    result = {
        Types.integer: data_type_int,
        Types.int32: data_type_int,
        Types.int64: data_type_int,
        Types.number: data_type_float,
        Types.float: data_type_float,
        Types.double: data_type_float,
        Types.decimal: data_type.from_import(IMPORT_DECIMAL),
        Types.time: data_type.from_import(IMPORT_TIME),
        Types.string: data_type_str,
        Types.byte: data_type_str,  # base64 encoded string
        Types.binary: data_type(type="bytes"),
        Types.date: data_type.from_import(IMPORT_DATE),
        Types.date_time: data_type.from_import(IMPORT_DATETIME),
        Types.timedelta: data_type.from_import(IMPORT_TIMEDELTA),
        Types.path: data_type.from_import(IMPORT_PATH),
        Types.password: data_type.from_import(IMPORT_SECRET_STR),
        Types.email: data_type.from_import(IMPORT_EMAIL_STR),
        Types.uuid: data_type.from_import(IMPORT_UUID),
        Types.uuid1: data_type.from_import(IMPORT_UUID1),
        Types.uuid2: data_type.from_import(IMPORT_UUID2),
        Types.uuid3: data_type.from_import(IMPORT_UUID3),
        Types.uuid4: data_type.from_import(IMPORT_UUID4),
        Types.uuid5: data_type.from_import(IMPORT_UUID5),
        Types.uri: data_type.from_import(IMPORT_ANYURL),
        Types.hostname: data_type.from_import(
            IMPORT_CONSTR,
            strict=StrictTypes.str in strict_types,
            # https://github.com/horejsek/python-fastjsonschema/blob/61c6997a8348b8df9b22e029ca2ba35ef441fbb8/fastjsonschema/draft04.py#L31
            kwargs={
                pattern_key: r"r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])\.)*"
                r"([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z0-9])\Z'",
                **({"strict": True} if StrictTypes.str in strict_types else {}),
            },
        ),
        Types.ipv4: data_type.from_import(IMPORT_IPV4ADDRESS),
        Types.ipv6: data_type.from_import(IMPORT_IPV6ADDRESS),
        Types.ipv4_network: data_type.from_import(IMPORT_IPV4NETWORKS),
        Types.ipv6_network: data_type.from_import(IMPORT_IPV6NETWORKS),
        Types.boolean: data_type(type="bool"),
        Types.object: data_type.from_import(IMPORT_ANY, is_dict=True),
        Types.null: data_type(type="None"),
        Types.array: data_type.from_import(IMPORT_ANY, is_list=True),
        Types.any: data_type.from_import(IMPORT_ANY),
    }
    if use_pendulum:
        result[Types.date] = data_type.from_import(IMPORT_PENDULUM_DATE)
        result[Types.date_time] = data_type.from_import(IMPORT_PENDULUM_DATETIME)
        result[Types.time] = data_type.from_import(IMPORT_PENDULUM_TIME)
        result[Types.timedelta] = data_type.from_import(IMPORT_PENDULUM_DURATION)

    return result


def strict_type_map_factory(data_type: type[DataType]) -> dict[StrictTypes, DataType]:
    return {
        StrictTypes.int: data_type.from_import(IMPORT_STRICT_INT, strict=True),
        StrictTypes.float: data_type.from_import(IMPORT_STRICT_FLOAT, strict=True),
        StrictTypes.bytes: data_type.from_import(IMPORT_STRICT_BYTES, strict=True),
        StrictTypes.bool: data_type.from_import(IMPORT_STRICT_BOOL, strict=True),
        StrictTypes.str: data_type.from_import(IMPORT_STRICT_STR, strict=True),
    }


number_kwargs: set[str] = {
    "exclusiveMinimum",
    "minimum",
    "exclusiveMaximum",
    "maximum",
    "multipleOf",
}

string_kwargs: set[str] = {"minItems", "maxItems", "minLength", "maxLength", "pattern"}

bytes_kwargs: set[str] = {"minLength", "maxLength"}

escape_characters = str.maketrans({
    "'": r"\'",
    "\b": r"\b",
    "\f": r"\f",
    "\n": r"\n",
    "\r": r"\r",
    "\t": r"\t",
})


class DataTypeManager(_DataTypeManager):
    PATTERN_KEY: ClassVar[str] = "regex"

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

        self.type_map: dict[Types, DataType] = self.type_map_factory(
            self.data_type,
            strict_types=self.strict_types,
            pattern_key=self.PATTERN_KEY,
            target_datetime_class=self.target_datetime_class,
        )
        self.strict_type_map: dict[StrictTypes, DataType] = strict_type_map_factory(
            self.data_type,
        )

        self.kwargs_schema_to_model: dict[str, str] = {
            "exclusiveMinimum": "gt",
            "minimum": "ge",
            "exclusiveMaximum": "lt",
            "maximum": "le",
            "multipleOf": "multiple_of",
            "minItems": "min_items",
            "maxItems": "max_items",
            "minLength": "min_length",
            "maxLength": "max_length",
            "pattern": self.PATTERN_KEY,
        }

    def type_map_factory(
        self,
        data_type: type[DataType],
        strict_types: Sequence[StrictTypes],
        pattern_key: str,
        target_datetime_class: DatetimeClassType | None,  # noqa: ARG002
    ) -> dict[Types, DataType]:
        return type_map_factory(
            data_type,
            strict_types,
            pattern_key,
            self.use_pendulum,
        )

    def transform_kwargs(self, kwargs: dict[str, Any], filter_: set[str]) -> dict[str, str]:
        return {self.kwargs_schema_to_model.get(k, k): v for (k, v) in kwargs.items() if v is not None and k in filter_}

    def get_data_int_type(  # noqa: PLR0911
        self,
        types: Types,
        **kwargs: Any,
    ) -> DataType:
        data_type_kwargs: dict[str, Any] = self.transform_kwargs(kwargs, number_kwargs)
        strict = StrictTypes.int in self.strict_types
        if data_type_kwargs:
            if not strict:
                if data_type_kwargs == {"gt": 0}:
                    return self.data_type.from_import(IMPORT_POSITIVE_INT)
                if data_type_kwargs == {"lt": 0}:
                    return self.data_type.from_import(IMPORT_NEGATIVE_INT)
                if data_type_kwargs == {"ge": 0} and self.use_non_positive_negative_number_constrained_types:
                    return self.data_type.from_import(IMPORT_NON_NEGATIVE_INT)
                if data_type_kwargs == {"le": 0} and self.use_non_positive_negative_number_constrained_types:
                    return self.data_type.from_import(IMPORT_NON_POSITIVE_INT)
            kwargs = {k: int(v) for k, v in data_type_kwargs.items()}
            if strict:
                kwargs["strict"] = True
            return self.data_type.from_import(IMPORT_CONINT, kwargs=kwargs)
        if strict:
            return self.strict_type_map[StrictTypes.int]
        return self.type_map[types]

    def get_data_float_type(  # noqa: PLR0911
        self,
        types: Types,
        **kwargs: Any,
    ) -> DataType:
        data_type_kwargs = self.transform_kwargs(kwargs, number_kwargs)
        strict = StrictTypes.float in self.strict_types
        if data_type_kwargs:
            if not strict:
                if data_type_kwargs == {"gt": 0}:
                    return self.data_type.from_import(IMPORT_POSITIVE_FLOAT)
                if data_type_kwargs == {"lt": 0}:
                    return self.data_type.from_import(IMPORT_NEGATIVE_FLOAT)
                if data_type_kwargs == {"ge": 0} and self.use_non_positive_negative_number_constrained_types:
                    return self.data_type.from_import(IMPORT_NON_NEGATIVE_FLOAT)
                if data_type_kwargs == {"le": 0} and self.use_non_positive_negative_number_constrained_types:
                    return self.data_type.from_import(IMPORT_NON_POSITIVE_FLOAT)
            kwargs = {k: float(v) for k, v in data_type_kwargs.items()}
            if strict:
                kwargs["strict"] = True
            return self.data_type.from_import(IMPORT_CONFLOAT, kwargs=kwargs)
        if strict:
            return self.strict_type_map[StrictTypes.float]
        return self.type_map[types]

    def get_data_decimal_type(self, types: Types, **kwargs: Any) -> DataType:
        data_type_kwargs = self.transform_kwargs(kwargs, number_kwargs)
        if data_type_kwargs:
            return self.data_type.from_import(
                IMPORT_CONDECIMAL,
                kwargs={k: Decimal(str(v) if isinstance(v, UnionIntFloat) else v) for k, v in data_type_kwargs.items()},
            )
        return self.type_map[types]

    def get_data_str_type(self, types: Types, **kwargs: Any) -> DataType:
        data_type_kwargs: dict[str, Any] = self.transform_kwargs(kwargs, string_kwargs)
        strict = StrictTypes.str in self.strict_types
        if data_type_kwargs:
            if strict:
                data_type_kwargs["strict"] = True
            if self.PATTERN_KEY in data_type_kwargs:
                escaped_regex = data_type_kwargs[self.PATTERN_KEY].translate(escape_characters)
                # TODO: remove unneeded escaped characters
                data_type_kwargs[self.PATTERN_KEY] = f"r'{escaped_regex}'"
            return self.data_type.from_import(IMPORT_CONSTR, kwargs=data_type_kwargs)
        if strict:
            return self.strict_type_map[StrictTypes.str]
        return self.type_map[types]

    def get_data_bytes_type(self, types: Types, **kwargs: Any) -> DataType:
        data_type_kwargs: dict[str, Any] = self.transform_kwargs(kwargs, bytes_kwargs)
        strict = StrictTypes.bytes in self.strict_types
        if data_type_kwargs and not strict:
            return self.data_type.from_import(IMPORT_CONBYTES, kwargs=data_type_kwargs)
        # conbytes doesn't accept strict argument
        # https://github.com/samuelcolvin/pydantic/issues/2489
        if strict:
            return self.strict_type_map[StrictTypes.bytes]
        return self.type_map[types]

    def get_data_type(  # noqa: PLR0911
        self,
        types: Types,
        **kwargs: Any,
    ) -> DataType:
        if types == Types.string:
            return self.get_data_str_type(types, **kwargs)
        if types in {Types.int32, Types.int64, Types.integer}:
            return self.get_data_int_type(types, **kwargs)
        if types in {Types.float, Types.double, Types.number, Types.time}:
            return self.get_data_float_type(types, **kwargs)
        if types == Types.decimal:
            return self.get_data_decimal_type(types, **kwargs)
        if types == Types.binary:
            return self.get_data_bytes_type(types, **kwargs)
        if types == Types.boolean and StrictTypes.bool in self.strict_types:
            return self.strict_type_map[StrictTypes.bool]

        return self.type_map[types]
