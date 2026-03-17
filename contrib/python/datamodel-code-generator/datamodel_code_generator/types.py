from __future__ import annotations

import re
from abc import ABC, abstractmethod
from enum import Enum, auto
from functools import lru_cache
from itertools import chain
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

import pydantic
from packaging import version
from pydantic import StrictBool, StrictInt, StrictStr, create_model

from datamodel_code_generator.format import (
    DatetimeClassType,
    PythonVersion,
    PythonVersionMin,
)
from datamodel_code_generator.imports import (
    IMPORT_ABC_MAPPING,
    IMPORT_ABC_SEQUENCE,
    IMPORT_ABC_SET,
    IMPORT_DICT,
    IMPORT_FROZEN_SET,
    IMPORT_LIST,
    IMPORT_LITERAL,
    IMPORT_MAPPING,
    IMPORT_OPTIONAL,
    IMPORT_SEQUENCE,
    IMPORT_SET,
    IMPORT_UNION,
    Import,
)
from datamodel_code_generator.reference import Reference, _BaseModel
from datamodel_code_generator.util import PYDANTIC_V2, ConfigDict

if TYPE_CHECKING:
    import builtins
    from collections.abc import Iterable, Iterator, Sequence

if PYDANTIC_V2:
    from pydantic import GetCoreSchemaHandler
    from pydantic_core import core_schema

T = TypeVar("T")

OPTIONAL = "Optional"
OPTIONAL_PREFIX = f"{OPTIONAL}["

UNION = "Union"
UNION_PREFIX = f"{UNION}["
UNION_DELIMITER = ", "
UNION_PATTERN: Pattern[str] = re.compile(r"\s*,\s*")
UNION_OPERATOR_DELIMITER = " | "
UNION_OPERATOR_PATTERN: Pattern[str] = re.compile(r"\s*\|\s*")
NONE = "None"
ANY = "Any"
LITERAL = "Literal"
SEQUENCE = "Sequence"
FROZEN_SET = "FrozenSet"
MAPPING = "Mapping"
DICT = "Dict"
SET = "Set"
LIST = "List"
STANDARD_DICT = "dict"
STANDARD_LIST = "list"
STANDARD_SET = "set"
STR = "str"

NOT_REQUIRED = "NotRequired"
NOT_REQUIRED_PREFIX = f"{NOT_REQUIRED}["


class StrictTypes(Enum):
    str = "str"
    bytes = "bytes"
    int = "int"
    float = "float"
    bool = "bool"


class UnionIntFloat:
    def __init__(self, value: float) -> None:
        self.value: int | float = value

    def __int__(self) -> int:
        return int(self.value)

    def __float__(self) -> float:
        return float(self.value)

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def __get_validators__(cls) -> Iterator[Callable[[Any], Any]]:  # noqa: PLW3201
        yield cls.validate

    @classmethod
    def __get_pydantic_core_schema__(  # noqa: PLW3201
        cls, _source_type: Any, _handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        from_int_schema = core_schema.chain_schema(  # pyright: ignore[reportPossiblyUnboundVariable]
            [
                core_schema.union_schema(  # pyright: ignore[reportPossiblyUnboundVariable]
                    [core_schema.int_schema(), core_schema.float_schema()]  # pyright: ignore[reportPossiblyUnboundVariable]
                ),
                core_schema.no_info_plain_validator_function(cls.validate),  # pyright: ignore[reportPossiblyUnboundVariable]
            ]
        )

        return core_schema.json_or_python_schema(  # pyright: ignore[reportPossiblyUnboundVariable]
            json_schema=from_int_schema,
            python_schema=core_schema.union_schema(  # pyright: ignore[reportPossiblyUnboundVariable]
                [
                    # check if it's an instance first before doing any further work
                    core_schema.is_instance_schema(UnionIntFloat),  # pyright: ignore[reportPossiblyUnboundVariable]
                    from_int_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(  # pyright: ignore[reportPossiblyUnboundVariable]
                lambda instance: instance.value
            ),
        )

    @classmethod
    def validate(cls, v: Any) -> UnionIntFloat:
        if isinstance(v, UnionIntFloat):
            return v
        if not isinstance(v, (int, float)):  # pragma: no cover
            try:
                int(v)
                return cls(v)
            except (TypeError, ValueError):
                pass
            try:
                float(v)
                return cls(v)
            except (TypeError, ValueError):
                pass

            msg = f"{v} is not int or float"
            raise TypeError(msg)
        return cls(v)


def chain_as_tuple(*iterables: Iterable[T]) -> tuple[T, ...]:
    return tuple(chain(*iterables))


@lru_cache
def _remove_none_from_type(type_: str, split_pattern: Pattern[str], delimiter: str) -> list[str]:
    types: list[str] = []
    split_type: str = ""
    inner_count: int = 0
    for part in re.split(split_pattern, type_):
        if part == NONE:
            continue
        inner_count += part.count("[") - part.count("]")
        if split_type:
            split_type += delimiter
        if inner_count == 0:
            if split_type:
                types.append(f"{split_type}{part}")
            else:
                types.append(part)
            split_type = ""
            continue
        split_type += part
    return types


def _remove_none_from_union(type_: str, *, use_union_operator: bool) -> str:  # noqa: PLR0912
    if use_union_operator:
        if " | " not in type_:
            return type_
        separator = "|"
        inner_text = type_
    else:
        if not type_.startswith(UNION_PREFIX):
            return type_
        separator = ","
        inner_text = type_[len(UNION_PREFIX) : -1]

    parts = []
    inner_count = 0
    current_part = ""

    # With this variable we count any non-escaped round bracket, whenever we are inside a
    # constraint string expression. Once found a part starting with `constr(`, we increment
    # this counter for each non-escaped opening round bracket and decrement it for each
    # non-escaped closing round bracket.
    in_constr = 0

    # Parse union parts carefully to handle nested structures
    for char in inner_text:
        current_part += char
        if char == "[" and in_constr == 0:
            inner_count += 1
        elif char == "]" and in_constr == 0:
            inner_count -= 1
        elif char == "(":
            if current_part.strip().startswith("constr(") and current_part[-2] != "\\":
                # non-escaped opening round bracket found inside constraint string expression
                in_constr += 1
        elif char == ")":
            if in_constr > 0 and current_part[-2] != "\\":
                # non-escaped closing round bracket found inside constraint string expression
                in_constr -= 1
        elif char == separator and inner_count == 0 and in_constr == 0:
            part = current_part[:-1].strip()
            if part != NONE:
                # Process nested unions recursively
                # only UNION_PREFIX might be nested but not union_operator
                if not use_union_operator and part.startswith(UNION_PREFIX):
                    part = _remove_none_from_union(part, use_union_operator=False)
                parts.append(part)
            current_part = ""

    part = current_part.strip()
    if current_part and part != NONE:
        # only UNION_PREFIX might be nested but not union_operator
        if not use_union_operator and part.startswith(UNION_PREFIX):
            part = _remove_none_from_union(part, use_union_operator=False)
        parts.append(part)

    if not parts:
        return NONE
    if len(parts) == 1:
        return parts[0]

    if use_union_operator:
        return UNION_OPERATOR_DELIMITER.join(parts)

    return f"{UNION_PREFIX}{UNION_DELIMITER.join(parts)}]"


@lru_cache
def get_optional_type(type_: str, use_union_operator: bool) -> str:  # noqa: FBT001
    type_ = _remove_none_from_union(type_, use_union_operator=use_union_operator)

    if not type_ or type_ == NONE:
        return NONE
    if use_union_operator:
        return f"{type_} | {NONE}"
    return f"{OPTIONAL_PREFIX}{type_}]"


@runtime_checkable
class Modular(Protocol):
    @property
    def module_name(self) -> str:
        raise NotImplementedError


@runtime_checkable
class Nullable(Protocol):
    @property
    def nullable(self) -> bool:
        raise NotImplementedError


class DataType(_BaseModel):
    if PYDANTIC_V2:
        # TODO[pydantic]: The following keys were removed: `copy_on_model_validation`.
        # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
        model_config = ConfigDict(  # pyright: ignore[reportAssignmentType]
            extra="forbid",
            revalidate_instances="never",
        )
    else:
        if not TYPE_CHECKING:

            @classmethod
            def model_rebuild(cls) -> None:
                cls.update_forward_refs()

        class Config:
            extra = "forbid"
            copy_on_model_validation = False if version.parse(pydantic.VERSION) < version.parse("1.9.2") else "none"

    type: Optional[str] = None  # noqa: UP045
    reference: Optional[Reference] = None  # noqa: UP045
    data_types: list[DataType] = []  # noqa: RUF012
    is_func: bool = False
    kwargs: Optional[dict[str, Any]] = None  # noqa: UP045
    import_: Optional[Import] = None  # noqa: UP045
    python_version: PythonVersion = PythonVersionMin
    is_optional: bool = False
    is_dict: bool = False
    is_list: bool = False
    is_set: bool = False
    is_custom_type: bool = False
    literals: list[Union[StrictBool, StrictInt, StrictStr]] = []  # noqa: RUF012, UP007
    use_standard_collections: bool = False
    use_generic_container: bool = False
    use_union_operator: bool = False
    alias: Optional[str] = None  # noqa: UP045
    parent: Optional[Any] = None  # noqa: UP045
    children: list[Any] = []  # noqa: RUF012
    strict: bool = False
    dict_key: Optional[DataType] = None  # noqa: UP045
    treat_dot_as_module: bool = False

    _exclude_fields: ClassVar[set[str]] = {"parent", "children"}
    _pass_fields: ClassVar[set[str]] = {"parent", "children", "data_types", "reference"}

    @classmethod
    def from_import(  # noqa: PLR0913
        cls: builtins.type[DataTypeT],
        import_: Import,
        *,
        is_optional: bool = False,
        is_dict: bool = False,
        is_list: bool = False,
        is_set: bool = False,
        is_custom_type: bool = False,
        strict: bool = False,
        kwargs: dict[str, Any] | None = None,
    ) -> DataTypeT:
        return cls(
            type=import_.import_,
            import_=import_,
            is_optional=is_optional,
            is_dict=is_dict,
            is_list=is_list,
            is_set=is_set,
            is_func=bool(kwargs),
            is_custom_type=is_custom_type,
            strict=strict,
            kwargs=kwargs,
        )

    @property
    def unresolved_types(self) -> frozenset[str]:
        return frozenset(
            {t.reference.path for data_types in self.data_types for t in data_types.all_data_types if t.reference}
            | ({self.reference.path} if self.reference else set())
        )

    def replace_reference(self, reference: Reference | None) -> None:
        if not self.reference:  # pragma: no cover
            msg = f"`{self.__class__.__name__}.replace_reference()` can't be called when `reference` field is empty."
            raise Exception(msg)  # noqa: TRY002
        self_id = id(self)
        self.reference.children = [c for c in self.reference.children if id(c) != self_id]
        self.reference = reference
        if reference:
            reference.children.append(self)

    def remove_reference(self) -> None:
        self.replace_reference(None)

    @property
    def module_name(self) -> str | None:
        if self.reference and isinstance(self.reference.source, Modular):
            return self.reference.source.module_name
        return None  # pragma: no cover

    @property
    def full_name(self) -> str:
        module_name = self.module_name
        if module_name:
            return f"{module_name}.{self.reference.short_name if self.reference else ''}"
        return self.reference.short_name if self.reference else ""

    @property
    def all_data_types(self) -> Iterator[DataType]:
        for data_type in self.data_types:
            yield from data_type.all_data_types
        yield self

    @property
    def all_imports(self) -> Iterator[Import]:
        for data_type in self.data_types:
            yield from data_type.all_imports
        yield from self.imports

    @property
    def imports(self) -> Iterator[Import]:
        # Add base import if exists
        if self.import_:
            yield self.import_

        # Define required imports based on type features and conditions
        imports: tuple[tuple[bool, Import], ...] = (
            (self.is_optional and not self.use_union_operator, IMPORT_OPTIONAL),
            (len(self.data_types) > 1 and not self.use_union_operator, IMPORT_UNION),
            (bool(self.literals), IMPORT_LITERAL),
        )

        if self.use_generic_container:
            if self.use_standard_collections:
                imports = (
                    *imports,
                    (self.is_list, IMPORT_ABC_SEQUENCE),
                    (self.is_set, IMPORT_ABC_SET),
                    (self.is_dict, IMPORT_ABC_MAPPING),
                )
            else:
                imports = (
                    *imports,
                    (self.is_list, IMPORT_SEQUENCE),
                    (self.is_set, IMPORT_FROZEN_SET),
                    (self.is_dict, IMPORT_MAPPING),
                )
        elif not self.use_standard_collections:
            imports = (
                *imports,
                (self.is_list, IMPORT_LIST),
                (self.is_set, IMPORT_SET),
                (self.is_dict, IMPORT_DICT),
            )

        # Yield imports based on conditions
        for field, import_ in imports:
            if field and import_ != self.import_:
                yield import_

        # Propagate imports from any dict_key type
        if self.dict_key:
            yield from self.dict_key.imports

    def __init__(self, **values: Any) -> None:
        if not TYPE_CHECKING:
            super().__init__(**values)

        for type_ in self.data_types:
            if type_.type == ANY and type_.is_optional:
                if any(t for t in self.data_types if t.type != ANY):  # pragma: no cover
                    self.is_optional = True
                    self.data_types = [t for t in self.data_types if not (t.type == ANY and t.is_optional)]
                break  # pragma: no cover

        for data_type in self.data_types:
            if data_type.reference or data_type.data_types:
                data_type.parent = self

        if self.reference:
            self.reference.children.append(self)

    @property
    def type_hint(self) -> str:  # noqa: PLR0912, PLR0915
        type_: str | None = self.alias or self.type
        if not type_:
            if self.is_union:
                data_types: list[str] = []
                for data_type in self.data_types:
                    data_type_type = data_type.type_hint
                    if data_type_type in data_types:  # pragma: no cover
                        continue

                    if data_type_type == NONE:
                        self.is_optional = True
                        continue

                    non_optional_data_type_type = _remove_none_from_union(
                        data_type_type, use_union_operator=self.use_union_operator
                    )

                    if non_optional_data_type_type != data_type_type:
                        self.is_optional = True

                    data_types.append(non_optional_data_type_type)
                if len(data_types) == 1:
                    type_ = data_types[0]
                elif self.use_union_operator:
                    type_ = UNION_OPERATOR_DELIMITER.join(data_types)
                else:
                    type_ = f"{UNION_PREFIX}{UNION_DELIMITER.join(data_types)}]"
            elif len(self.data_types) == 1:
                type_ = self.data_types[0].type_hint
            elif self.literals:
                type_ = f"{LITERAL}[{', '.join(repr(literal) for literal in self.literals)}]"
            elif self.reference:
                type_ = self.reference.short_name
            else:
                # TODO support strict Any
                type_ = ""
        if self.reference:
            source = self.reference.source
            if isinstance(source, Nullable) and source.nullable:
                self.is_optional = True
        if self.is_list:
            if self.use_generic_container:
                list_ = SEQUENCE
            elif self.use_standard_collections:
                list_ = STANDARD_LIST
            else:
                list_ = LIST
            type_ = f"{list_}[{type_}]" if type_ else list_
        elif self.is_set:
            if self.use_generic_container:
                set_ = FROZEN_SET
            elif self.use_standard_collections:
                set_ = STANDARD_SET
            else:
                set_ = SET
            type_ = f"{set_}[{type_}]" if type_ else set_
        elif self.is_dict:
            if self.use_generic_container:
                dict_ = MAPPING
            elif self.use_standard_collections:
                dict_ = STANDARD_DICT
            else:
                dict_ = DICT
            if self.dict_key or type_:
                key = self.dict_key.type_hint if self.dict_key else STR
                type_ = f"{dict_}[{key}, {type_ or ANY}]"
            else:  # pragma: no cover
                type_ = dict_
        if self.is_optional and type_ != ANY:
            return get_optional_type(type_, self.use_union_operator)
        if self.is_func:
            if self.kwargs:
                kwargs: str = ", ".join(f"{k}={v}" for k, v in self.kwargs.items())
                return f"{type_}({kwargs})"
            return f"{type_}()"
        return type_

    @property
    def is_union(self) -> bool:
        return len(self.data_types) > 1


DataType.model_rebuild()

DataTypeT = TypeVar("DataTypeT", bound=DataType)


class EmptyDataType(DataType):
    pass


class Types(Enum):
    integer = auto()
    int32 = auto()
    int64 = auto()
    number = auto()
    float = auto()
    double = auto()
    decimal = auto()
    time = auto()
    string = auto()
    byte = auto()
    binary = auto()
    date = auto()
    date_time = auto()
    timedelta = auto()
    password = auto()
    path = auto()
    email = auto()
    uuid = auto()
    uuid1 = auto()
    uuid2 = auto()
    uuid3 = auto()
    uuid4 = auto()
    uuid5 = auto()
    uri = auto()
    hostname = auto()
    ipv4 = auto()
    ipv4_network = auto()
    ipv6 = auto()
    ipv6_network = auto()
    boolean = auto()
    object = auto()
    null = auto()
    array = auto()
    any = auto()


class DataTypeManager(ABC):
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
        self.python_version = python_version
        self.use_standard_collections: bool = use_standard_collections
        self.use_generic_container_types: bool = use_generic_container_types
        self.strict_types: Sequence[StrictTypes] = strict_types or ()
        self.use_non_positive_negative_number_constrained_types: bool = (
            use_non_positive_negative_number_constrained_types
        )
        self.use_union_operator: bool = use_union_operator
        self.use_pendulum: bool = use_pendulum
        self.target_datetime_class: DatetimeClassType | None = target_datetime_class
        self.treat_dot_as_module: bool = treat_dot_as_module

        if TYPE_CHECKING:
            self.data_type: type[DataType]
        else:
            self.data_type: type[DataType] = create_model(
                "ContextDataType",
                python_version=(PythonVersion, python_version),
                use_standard_collections=(bool, use_standard_collections),
                use_generic_container=(bool, use_generic_container_types),
                use_union_operator=(bool, use_union_operator),
                treat_dot_as_module=(bool, treat_dot_as_module),
                __base__=DataType,
            )

    @abstractmethod
    def get_data_type(self, types: Types, **kwargs: Any) -> DataType:
        raise NotImplementedError

    def get_data_type_from_full_path(self, full_path: str, is_custom_type: bool) -> DataType:  # noqa: FBT001
        return self.data_type.from_import(Import.from_full_path(full_path), is_custom_type=is_custom_type)

    def get_data_type_from_value(self, value: Any) -> DataType:
        type_: Types | None = None
        if isinstance(value, str):
            type_ = Types.string
        elif isinstance(value, bool):
            type_ = Types.boolean
        elif isinstance(value, int):
            type_ = Types.integer
        elif isinstance(value, float):
            type_ = Types.float
        elif isinstance(value, dict):
            return self.data_type.from_import(IMPORT_DICT)
        elif isinstance(value, list):
            return self.data_type.from_import(IMPORT_LIST)
        else:
            type_ = Types.any
        return self.get_data_type(type_)
