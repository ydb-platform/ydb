import ipaddress
import uuid
from collections.abc import Mapping, Sequence
from enum import Enum as PyEnum
from typing import Any, TypeVar, cast, overload

from sqlalchemy.exc import ArgumentError, CompileError
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.types import (
    ARRAY,
    Float,
    Integer,
    Interval,
    Numeric,
    TypeEngine,
    UserDefinedType,
)
from sqlalchemy.types import (
    Boolean as SqlaBoolean,
)
from sqlalchemy.types import (
    Date as SqlaDate,
)
from sqlalchemy.types import (
    DateTime as SqlaDateTime,
)
from sqlalchemy.types import (
    String as SqlaString,
)

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType, sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.sql.clauses import json_subcolumn
from clickhouse_connect.datatypes.base import EMPTY_TYPE_DEF, LC_TYPE_DEF, NULLABLE_TYPE_DEF, TypeDef, _TypeArgs
from clickhouse_connect.datatypes.dynamic import Variant as ChVariant
from clickhouse_connect.datatypes.numeric import Enum8 as ChEnum8
from clickhouse_connect.datatypes.numeric import Enum16 as ChEnum16
from clickhouse_connect.datatypes.registry import _WRAPPER_TYPE_ARGS, get_from_name, type_map
from clickhouse_connect.datatypes.vector import QBit as ChQBit
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.binding import _format_identifier, format_str
from clickhouse_connect.driver.common import decimal_prec, unescape_identifier
from clickhouse_connect.driver.exceptions import ClickHouseError, InternalError
from clickhouse_connect.driver.parser import parse_callable, parse_columns, parse_enum

_T = TypeVar("_T")

_TYPE_ARGS = {name.lower(): ch_type._type_args for name, ch_type in type_map.items()}
_TYPE_ARGS.update({name.lower(): type_args for name, type_args in _WRAPPER_TYPE_ARGS.items()})
_ZERO_ARGUMENT_TYPE_NAMES = frozenset(name for name, type_args in _TYPE_ARGS.items() if type_args.min_args == 0 and type_args.max_args == 0)
_CANONICAL_TYPE_NAMES = {name.lower(): name for name in type_map}
_CANONICAL_TYPE_NAMES.update({"boolean": "Bool", "enum": "Enum", "lowcardinality": "LowCardinality", "nullable": "Nullable"})
_INTERNAL_CODEC_TYPE_NAMES = frozenset({"shareddatastring", "sharedvariant"})


def _has_valid_arity(type_args: _TypeArgs, arg_count: int) -> bool:
    return arg_count >= type_args.min_args and (type_args.max_args is None or arg_count <= type_args.max_args)


def _integer_bounds(type_args: _TypeArgs, index: int | str) -> tuple[int | None, int | None]:
    bounds = type_args.bounds_for(index)
    if bounds is None:
        raise InternalError(f"Missing integer bounds for type argument {index!r}")
    return bounds


def _is_integer_in_bounds(value: object, bounds: tuple[int | None, int | None]) -> bool:
    if not isinstance(value, int):
        return False
    minimum, maximum = bounds
    return (minimum is None or value >= minimum) and (maximum is None or value <= maximum)


class Int8(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class UInt8(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class Int16(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class UInt16(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class Int32(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class UInt32(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class Int64(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class UInt64(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class Int128(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class UInt128(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class Int256(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class UInt256(ChSqlaType, Integer):  # type: ignore[misc]
    pass


class Float32(ChSqlaType, Float):  # type: ignore[misc]
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        ChSqlaType.__init__(self, type_def)
        Float.__init__(self)


class Float64(ChSqlaType, Float):  # type: ignore[misc]
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        ChSqlaType.__init__(self, type_def)
        Float.__init__(self)


class Bool(ChSqlaType, SqlaBoolean):  # type: ignore[misc]
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF, **kwargs):
        ChSqlaType.__init__(self, type_def)
        SqlaBoolean.__init__(self, **kwargs)


class Boolean(Bool):
    pass


class Decimal(ChSqlaType, Numeric):  # type: ignore[misc]
    dec_size = 0

    def __init__(self, precision: int = 0, scale: int = 0, type_def: TypeDef | None = None):
        """
        Construct either with precision and scale (for DDL), or a TypeDef with those values (by name)
        :param precision:  Number of digits the Decimal
        :param scale: Digits after the decimal point
        :param type_def: Parsed type def from ClickHouse arguments
        """
        if type_def:
            if self.dec_size:
                precision = decimal_prec[self.dec_size]
                scale = type_def.values[0]
            else:
                precision, scale = type_def.values
        elif not precision or scale < 0 or scale > precision:
            raise ArgumentError("Invalid precision or scale for ClickHouse Decimal type")
        else:
            type_def = TypeDef(values=(precision, scale))
        ChSqlaType.__init__(self, type_def)
        Numeric.__init__(self, precision, scale)


class Decimal32(Decimal):
    dec_size = 32


class Decimal64(Decimal):
    dec_size = 64


class Decimal128(Decimal):
    dec_size = 128


class Decimal256(Decimal):
    dec_size = 256


class Enum(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    _size = 16
    python_type = str

    def __init__(
        self,
        enum: type[PyEnum] | None = None,
        keys: Sequence[str] | None = None,
        values: Sequence[int] | None = None,
        type_def: TypeDef | None = None,
    ):
        """
        Construct a ClickHouse enum either from a Python Enum or parallel lists of keys and value.  Note that
        Python enums do not support empty strings as keys, so the alternate keys/values must be used in that case
        :param enum: Python enum to convert
        :param keys: List of string keys
        :param values: List of integer values
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if enum:
                keys = [e.name for e in enum]
                values = [e.value for e in enum]
            if keys is None or values is None:
                raise ArgumentError("Enum requires either a Python enum or both 'keys' and 'values'")
            self._validate(keys, values)
            if self.__class__.__name__ == "Enum":
                if max(values) <= 127 and min(values) >= -128:
                    self._ch_type_cls = ChEnum8
                else:
                    self._ch_type_cls = ChEnum16
            type_def = TypeDef(keys=tuple(keys), values=tuple(values))
        super().__init__(type_def)

    @classmethod
    def _validate(cls, keys: Sequence[str], values: Sequence[int]):
        bad_key = next((x for x in keys if not isinstance(x, str)), None)
        if bad_key:
            raise ArgumentError(f"ClickHouse enum key {bad_key} is not a string")
        bad_value = next((x for x in values if not isinstance(x, int)), None)
        if bad_value:
            raise ArgumentError(f"ClickHouse enum value {bad_value} is not an integer")
        value_min = -(2 ** (cls._size - 1))
        value_max = 2 ** (cls._size - 1) - 1
        bad_value = next((x for x in values if x < value_min or x > value_max), None)
        if bad_value:
            raise ArgumentError(f"Clickhouse enum value {bad_value} is out of range")


class Enum8(Enum):
    _size = 8
    _ch_type_cls = ChEnum8


class Enum16(Enum):
    _ch_type_cls = ChEnum16


class String(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = str


class FixedString(ChSqlaType, SqlaString):  # type: ignore[misc]
    def __init__(self, size: int = -1, type_def: TypeDef | None = None):
        if not type_def:
            type_def = TypeDef(values=(size,))
        ChSqlaType.__init__(self, type_def)
        SqlaString.__init__(self, size)


class IPv4(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = ipaddress.IPv4Address


class IPv6(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = ipaddress.IPv6Address


class UUID(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = uuid.UUID


class Nothing(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = type(None)


class Point(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = tuple


class Ring(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list


class Polygon(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list


class MultiPolygon(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list


class LineString(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list


class MultiLineString(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list


class MultiPoint(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list


class Geometry(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = object


class Date(ChSqlaType, SqlaDate):  # type: ignore[misc]
    pass


class Date32(ChSqlaType, SqlaDate):  # type: ignore[misc]
    pass


_TIMEZONE_SENTINEL = object()


def _resolve_tz_alias(tz, timezone):
    """Resolve `tz=` / `timezone=` alias (clickhouse-sqlalchemy naming). Returns the zone string or None.

    timezone=False maps silently to None: SQLAlchemy's type-adaptation passes it when cloning
    DateTime (inherited from SqlaDateTime.timezone default). timezone=True is rejected because
    ClickHouse requires a concrete IANA zone.
    """
    if timezone is not _TIMEZONE_SENTINEL:
        if timezone is True:
            raise ArgumentError(
                "timezone=True is not supported for ClickHouse DateTime types; "
                "pass a named IANA zone string such as timezone='UTC' or timezone='America/New_York'"
            )
        if timezone is False:
            return tz
        if tz is not None:
            raise ArgumentError("Cannot specify both 'tz' and 'timezone'; they are aliases")
        return timezone
    return tz


class DateTime(ChSqlaType, SqlaDateTime):  # type: ignore[misc]
    def __init__(self, tz: str | None = None, type_def: TypeDef | None = None, timezone=_TIMEZONE_SENTINEL):
        """tz / timezone: IANA zone string (resolved via zoneinfo; install `tzdata` on Windows)."""
        tz = _resolve_tz_alias(tz, timezone)
        if not type_def:
            if tz:
                tzutil.resolve_zone(tz)
                type_def = TypeDef(values=(f"'{tz}'",))
            else:
                type_def = EMPTY_TYPE_DEF
        ChSqlaType.__init__(self, type_def)
        SqlaDateTime.__init__(self)


class DateTime64(ChSqlaType, SqlaDateTime):  # type: ignore[misc]
    def __init__(self, precision: int | None = None, tz: str | None = None, type_def: TypeDef | None = None, timezone=_TIMEZONE_SENTINEL):
        """precision: 3/6/9 for ms/us/ns. tz / timezone: IANA zone string."""
        tz = _resolve_tz_alias(tz, timezone)
        if not type_def:
            if tz:
                tzutil.resolve_zone(tz)
                type_def = TypeDef(values=(precision, f"'{tz}'"))
            else:
                type_def = TypeDef(values=(precision,))
        prec = type_def.values[0] if len(type_def.values) else None
        if not isinstance(prec, int) or prec < 0 or prec > 9:
            raise ArgumentError(f"Invalid precision value {prec} for ClickHouse DateTime64")
        ChSqlaType.__init__(self, type_def)
        SqlaDateTime.__init__(self)


class Time(ChSqlaType, Interval):  # type: ignore[misc]
    """
    Represents the ClickHouse Time type, which corresponds to a timedelta.

    Represents time durations in the range -999:59:59 to 999:59:59 with
    second precision. Maps to Python timedelta objects.
    """

    cache_ok = True

    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        ChSqlaType.__init__(self, type_def)
        Interval.__init__(self)

    def bind_processor(self, dialect):
        return None

    def coerce_compared_value(self, op, value):
        return self


class Time64(ChSqlaType, Interval):  # type: ignore[misc]
    """
    Represents the ClickHouse Time64 type with configurable precision.

    Represents time durations in the range -999:59:59.999999999 to
    999:59:59.999999999 configurable precision. Maps to Python timedelta objects.
    If no precision is defined it default to 3.
    """

    cache_ok = True

    def __init__(self, precision: int | None = None, type_def: TypeDef | None = None):
        """
        Time64 constructor with precision if not constructed with TypeDef.
        :param precision: Number of fractional second digits from 0 through 9.
        :param type_def: TypeDef from parse_name function.
        """
        if not type_def:
            if precision is None:
                precision = 3
            type_def = TypeDef(values=(precision,))
        else:
            precision = type_def.values[0] if len(type_def.values) > 0 else 3

        if isinstance(precision, bool) or not isinstance(precision, int) or precision < 0 or precision > 9:
            raise ArgumentError(f"Invalid precision value {precision} for ClickHouse Time64. Must be an integer from 0 through 9.")

        ChSqlaType.__init__(self, type_def)

        Interval.__init__(self, second_precision=precision)

    def bind_processor(self, dialect):
        return None

    def coerce_compared_value(self, op, value):
        return self


def Nullable(element: ChSqlaType | type[ChSqlaType]) -> ChSqlaType:  # noqa: N802
    """Wrap a ChSqlaType instance or class with a Nullable modifier for DDL construction."""
    if callable(element):
        return element(type_def=NULLABLE_TYPE_DEF)
    orig = element.type_def
    wrappers = orig.wrappers if "Nullable" in orig.wrappers else orig.wrappers + ("Nullable",)
    return element.__class__(type_def=TypeDef(wrappers, orig.keys, orig.values))


def LowCardinality(element: ChSqlaType | type[ChSqlaType]) -> ChSqlaType:  # noqa: N802
    """Wrap a ChSqlaType instance or class with a LowCardinality modifier for DDL construction."""
    if callable(element):
        return element(type_def=LC_TYPE_DEF)
    orig = element.type_def
    wrappers = orig.wrappers if "LowCardinality" in orig.wrappers else ("LowCardinality",) + orig.wrappers
    return element.__class__(type_def=TypeDef(wrappers, orig.keys, orig.values))


class Array(ChSqlaType, ARRAY):  # type: ignore[misc]
    python_type = list
    dimensions = 1

    def __init__(self, element: ChSqlaType | type[ChSqlaType] | None = None, type_def: TypeDef | None = None):
        """
        Array constructor that can take a wrapped Array type if not constructed from a TypeDef
        :param element: ChSqlaType instance or class to wrap
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if callable(element):
                element = element()
            if element is None:
                raise ArgumentError("Array requires an element type or type_def")
            type_def = TypeDef(values=(element.name,))
        ChSqlaType.__init__(self, type_def)
        # Set item_type directly; calling ARRAY.__init__ would reject nested Array(Array(T)),
        # which CH supports natively (CH expresses dimensions via nesting, not a dim count).
        # as_tuple has no class-level default and ARRAY reads it (e.g. the hashable property), so set it
        # here since we skip ARRAY.__init__.
        self.item_type = cast("TypeEngine[Any]", sqla_type_from_name(type_def.values[0]))
        self.as_tuple = False


class Map(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = dict

    def __init__(
        self,
        key_type: ChSqlaType | type[ChSqlaType] | None = None,
        value_type: ChSqlaType | type[ChSqlaType] | None = None,
        type_def: TypeDef | None = None,
    ):
        """
        Map constructor that can take a wrapped key/values types if not constructed from a TypeDef
        :param key_type: ChSqlaType instance or class to use as keys for the Map
        :param value_type: ChSqlaType instance or class to use as values for the Map
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if callable(key_type):
                key_type = key_type()
            if callable(value_type):
                value_type = value_type()
            if key_type is None or value_type is None:
                raise ArgumentError("Map requires key_type and value_type, or type_def")
            type_def = TypeDef(values=(key_type.name, value_type.name))
        super().__init__(type_def)


class Tuple(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = tuple

    def __init__(
        self,
        *args,
        elements: Sequence[ChSqlaType | type[ChSqlaType]] | None = None,
        type_def: TypeDef | None = None,
    ):
        """Tuple(UInt32, UUID) variadic form or Tuple(elements=[UInt32, UUID]) list form, not both."""
        if type_def is None and not args and elements is None:
            # SA's dialect_impl -> adapt -> constructor_copy can call cls() with no args
            # because get_cls_kwargs doesn't see keyword-only args behind *args.
            # adapt() below preserves the real type_def; this branch just avoids a crash.
            type_def = EMPTY_TYPE_DEF
        if not type_def:
            if args and elements is not None:
                raise ArgumentError("Cannot specify both positional elements and the 'elements' kwarg")
            if args:
                elements = args
            values = [et() if callable(et) else et for et in elements]  # type: ignore[union-attr]
            type_def = TypeDef(values=tuple(v.name for v in values))
        super().__init__(type_def)

    def adapt(self, cls, **kw):
        # Bypass SA's constructor_copy: it can't see keyword-only args behind *args and
        # would produce an empty Tuple. Copy state directly.
        inst = cls.__new__(cls)
        inst.__dict__.update(self.__dict__)
        return inst


class _Variant(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    _schema_name = "Variant"
    python_type = object

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.type_def = cast(ChVariant, self.ch_type).type_def

    def adapt(self, cls, **kw):
        inst = cls.__new__(cls)
        inst.__dict__.update(self.__dict__)
        return inst


class JSON(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    cache_ok: bool = True
    python_type: type[dict[str, object]] = dict

    def __init__(
        self,
        type_def: TypeDef | None = None,
        *,
        typed_paths: Mapping[str, str | ChSqlaType | type[ChSqlaType]] | None = None,
        max_dynamic_paths: int | None = None,
        max_dynamic_types: int | None = None,
        skip_paths: Sequence[str] | None = None,
        skip_regexps: Sequence[str] | None = None,
        **path_types: str | ChSqlaType | type[ChSqlaType],
    ):
        """Construct a ClickHouse JSON type with optional typed and skipped paths."""
        if type_def is not None:
            if not isinstance(type_def, TypeDef):
                raise ArgumentError("type_def must be a TypeDef instance")
            if (
                any(value is not None for value in (typed_paths, max_dynamic_paths, max_dynamic_types, skip_paths, skip_regexps))
                or path_types
            ):
                raise ArgumentError("type_def cannot be combined with JSON constructor arguments")
            super().__init__(type_def)
            return

        max_dynamic_paths = self._validate_limit("max_dynamic_paths", max_dynamic_paths, 10000)
        max_dynamic_types = self._validate_limit("max_dynamic_types", max_dynamic_types, 254)
        if typed_paths is not None and not isinstance(typed_paths, Mapping):
            raise ArgumentError("typed_paths must be a mapping of path names to ClickHouse types")
        typed = dict(typed_paths or {})
        duplicates = typed.keys() & path_types.keys()
        if duplicates:
            duplicate = sorted(duplicates)[0]
            raise ArgumentError(f"JSON typed path {duplicate!r} was provided more than once")
        typed.update(path_types)
        if len(typed) > 1000:
            raise ArgumentError("JSON accepts at most 1000 typed paths")

        normalized_typed: list[tuple[str, str]] = []
        for path, type_hint in typed.items():
            if not isinstance(path, str):
                raise ArgumentError("JSON typed path names must be strings")
            if not path:
                raise ArgumentError("JSON typed path names cannot be empty")
            normalized_typed.append((path, self._type_hint_name(type_hint)))
        normalized_typed.sort(key=lambda item: item[0])
        normalized_skip_paths = sorted(set(self._validate_paths("skip_paths", skip_paths)))
        normalized_skip_regexps = sorted(self._validate_paths("skip_regexps", skip_regexps))

        keys: list[str] = []
        values: list[str | int] = []
        if max_dynamic_types is not None and max_dynamic_types != 32:
            keys.append("max_dynamic_types")
            values.append(max_dynamic_types)
        if max_dynamic_paths is not None and max_dynamic_paths != 1024:
            keys.append("max_dynamic_paths")
            values.append(max_dynamic_paths)
        for path, type_name in normalized_typed:
            keys.append(_format_identifier(path))
            values.append(type_name)
        for path in normalized_skip_paths:
            keys.append("SKIP")
            values.append(_format_identifier(path))
        for regexp in normalized_skip_regexps:
            keys.append("SKIP")
            values.append(f"REGEXP {format_str(regexp)}")
        # SQLAlchemy copy and cache reconstruction retain only constructor state encoded in type_def.
        super().__init__(TypeDef(keys=tuple(keys), values=tuple(values)))

    @staticmethod
    def _validate_limit(name: str, value: int | None, maximum: int) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > maximum:
            raise ArgumentError(f"{name} must be an integer from 0 through {maximum}")
        return value

    @staticmethod
    def _validate_paths(name: str, paths: Sequence[str] | None) -> list[str]:
        if paths is None:
            return []
        if isinstance(paths, str) or any(not isinstance(path, str) for path in paths):
            raise ArgumentError(f"{name} must be a sequence of strings")
        if name == "skip_paths":
            if any(not path for path in paths):
                raise ArgumentError("JSON skip path names cannot be empty")
            if any(path.lower() == "regexp" for path in paths):
                raise ArgumentError("JSON skip path 'REGEXP' conflicts with the SKIP REGEXP syntax")
        return list(paths)

    @staticmethod
    def _type_hint_name(type_hint: str | ChSqlaType | type[ChSqlaType]) -> str:
        if isinstance(type_hint, ChSqlaType):
            return JSON._configured_type_name(type_hint)
        if isinstance(type_hint, type) and issubclass(type_hint, ChSqlaType):
            try:
                return JSON._configured_type_name(type_hint())
            except (ArgumentError, ClickHouseError, CompileError, IndexError, TypeError, ValueError) as ex:
                raise ArgumentError(f"JSON type class {type_hint.__name__} requires constructor arguments") from ex
        if isinstance(type_hint, str):
            try:
                type_hint = JSON._canonicalize_type_spelling(type_hint.strip())
                JSON._validate_type_expression(type_hint)
                return get_from_name(type_hint).name
            except (ArithmeticError, ClickHouseError, CompileError, IndexError, TypeError, ValueError) as ex:
                raise ArgumentError(f"Invalid ClickHouse type hint {type_hint!r}") from ex
        raise ArgumentError("JSON typed path values must be ClickHouse SQLAlchemy types or type name strings")

    @staticmethod
    def _canonicalize_type_spelling(type_name: str) -> str:
        base, args, remaining = parse_callable(type_name)
        canonical_base = _CANONICAL_TYPE_NAMES.get(base.lower(), base)
        opening = type_name.find("(")
        if remaining:
            return canonical_base + type_name[len(base) :]
        type_base = base.lower()
        if opening == -1:
            if type_base == "tuple":
                return "Tuple()"
            if type_base in ("datetime64", "time64"):
                return f"{canonical_base}(3)"
            if type_base == "decimal":
                return "Decimal(10, 0)"
            return canonical_base

        if type_base in ("datetime64", "time64") and not args:
            return f"{canonical_base}(3)"
        if type_base == "tuple" and not args:
            return "Tuple()"
        if type_base == "decimal" and not args:
            return "Decimal(10, 0)"
        type_args = _TYPE_ARGS.get(type_base)
        if not args and type_args is not None and type_args.min_args == 0:
            return canonical_base
        if type_base == "datetime" and args and isinstance(args[0], int):
            if args[0] == 0:
                if len(args) == 1:
                    return "DateTime"
                canonical_base = "DateTime"
                args = args[1:]
            else:
                canonical_base = "DateTime64"
                type_base = "datetime64"
        elif type_base == "time" and args and isinstance(args[0], int):
            if args[0] == 0:
                if len(args) == 1:
                    return "Time"
            else:
                canonical_base = "Time64"
                type_base = "time64"

        type_args = _TYPE_ARGS.get(type_base)

        if type_base in ("tuple", "nested"):
            names, raw_values = parse_columns(type_name[opening:], preserve_names=True)
            if names and len(names) != len(raw_values):
                if type_base == "nested":
                    raise ArgumentError("Nested requires named elements")
                raise ArgumentError("Tuple elements must be all named or all unnamed")
            if names:
                values = [
                    f"{_format_identifier(unescape_identifier(name))} {JSON._canonicalize_type_spelling(value)}"
                    for name, value in zip(names, raw_values)
                ]
            else:
                values = [JSON._canonicalize_type_spelling(value) for value in raw_values]
        elif type_base == "json":
            names, raw_values = parse_columns(type_name[opening:], preserve_names=True)
            if len(names) != len(raw_values):
                return canonical_base + type_name[opening:]
            values = []
            for name, value in zip(names, raw_values):
                quoted_name = JSON._is_quoted_identifier(name)
                path = unescape_identifier(name)
                if not quoted_name and path.upper() == "SKIP":
                    if JSON._starts_regexp_directive(value):
                        value = f"REGEXP {value[6:].lstrip()}"
                elif not quoted_name and type_args is not None and type_args.bounds_for(path) is not None:
                    pass
                else:
                    value = JSON._canonicalize_type_spelling(value)
                values.append(f"{name} {value}")
        elif type_base in ("decimal32", "decimal64", "decimal128", "decimal256") and len(args) == 1:
            _, max_scale = _integer_bounds(_TYPE_ARGS[type_base], 0)
            return f"Decimal({max_scale}, {args[0]})"
        elif type_base in ("enum", "enum8", "enum16"):
            return canonical_base + type_name[opening:]
        elif type_base == "decimal" and len(args) == 1:
            values = [str(args[0]), "0"]
        elif type_args is not None:
            nested_indexes = type_args.nested_indexes(len(args))
            values = [
                JSON._canonicalize_type_spelling(str(value)) if index in nested_indexes else str(value) for index, value in enumerate(args)
            ]
        else:
            values = [str(value) for value in args]
        return f"{canonical_base}({', '.join(values)})"

    @staticmethod
    def _validate_type_expression(type_name: str) -> None:
        if not type_name:
            raise ArgumentError("ClickHouse type hints cannot be empty")
        base, args, remaining = parse_callable(type_name)
        if remaining:
            raise ArgumentError(f"Unexpected trailing text {remaining!r}")

        type_base = base.lower()
        opening = type_name.find("(")
        type_args = _TYPE_ARGS.get(type_base)
        if type_base in _INTERNAL_CODEC_TYPE_NAMES:
            raise ArgumentError(f"{base} is an internal codec type and cannot be used in DDL")
        if type_base in _ZERO_ARGUMENT_TYPE_NAMES:
            if opening == -1 or not args:
                return
            raise ArgumentError(f"{base} does not accept these constructor arguments")
        nested_types: Sequence[str | int]
        if type_args is not None and type_args.min_args == type_args.max_args and type_args.nested == tuple(range(type_args.min_args)):
            expected_arity = type_args.min_args
            if not _has_valid_arity(type_args, len(args)):
                argument = "argument" if expected_arity == 1 else "arguments"
                raise ArgumentError(f"{base} requires exactly {expected_arity} type {argument}")
            nested_types = tuple(args[index] for index in type_args.nested_indexes(len(args)))
        elif type_base in ("tuple", "variant", "nested"):
            if type_base == "tuple" and not args:
                raise ArgumentError("Tuple() is not supported as a JSON typed path")
            assert type_args is not None
            if opening == -1 or not _has_valid_arity(type_args, len(args)):
                raise ArgumentError(f"{base} requires constructor arguments")
            names, nested_types = parse_columns(type_name[opening:])
            if type_base == "tuple" and names and len(names) != len(nested_types):
                raise ArgumentError("Tuple elements must be all named or all unnamed")
            if type_base == "nested" and len(names) != len(nested_types):
                raise ArgumentError("Nested requires a name for every element")
            if type_base == "variant" and names:
                raise ArgumentError("Variant members cannot be named")
            nested_types = tuple(nested_types[index] for index in type_args.nested_indexes(len(nested_types)))
        elif type_base == "json":
            if opening == -1 or not args:
                return
            assert type_args is not None
            names, values = parse_columns(type_name[opening:], preserve_names=True)
            if len(names) != len(values):
                raise ArgumentError("JSON type arguments must be named")
            json_nested_types: list[str | int] = []
            for name, value in zip(names, values):
                quoted_name = len(name) >= 2 and name[0] in ('"', "`") and name[-1] == name[0]
                path = unescape_identifier(name)
                if not path:
                    raise ArgumentError("JSON path names cannot be empty")
                if not quoted_name and path.upper() == "SKIP":
                    skip_path = value
                    if JSON._starts_regexp_directive(skip_path):
                        if not JSON._is_string_literal(skip_path[6:].lstrip()):
                            raise ArgumentError("JSON SKIP REGEXP requires one quoted string literal")
                        continue
                    if not JSON._is_identifier_expression(skip_path):
                        raise ArgumentError("JSON SKIP requires exactly one path identifier")
                    unescaped_skip_path = unescape_identifier(skip_path)
                    if not unescaped_skip_path:
                        raise ArgumentError("JSON skip path names cannot be empty")
                    if unescaped_skip_path.lower() == "regexp":
                        raise ArgumentError("JSON skip path 'REGEXP' conflicts with the SKIP REGEXP syntax")
                    continue
                bounds = type_args.bounds_for(path)
                if not quoted_name and bounds is not None:
                    try:
                        limit = int(value)
                    except (TypeError, ValueError):
                        raise ArgumentError(f"{path} must be an integer") from None
                    _, maximum = bounds
                    assert maximum is not None
                    JSON._validate_limit(path, limit, maximum)
                    continue
                json_nested_types.append(value)
            nested_types = json_nested_types
        elif type_base == "datetime":
            if opening == -1:
                return
            assert type_args is not None
            if len(args) != type_args.max_args or not isinstance(args[0], str) or not JSON._is_string_literal(args[0]):
                raise ArgumentError("DateTime accepts one quoted timezone name")
            return
        elif type_base == "datetime64":
            assert type_args is not None
            if not _has_valid_arity(type_args, len(args)):
                raise ArgumentError("DateTime64 requires a precision and optional quoted timezone name")
            precision = args[0]
            if not _is_integer_in_bounds(precision, _integer_bounds(type_args, 0)):
                raise ArgumentError("DateTime64 precision must be an integer from 0 through 9")
            if len(args) == 2 and (not isinstance(args[1], str) or not JSON._is_string_literal(args[1])):
                raise ArgumentError("DateTime64 timezone must be a quoted string literal")
            return
        elif type_base == "time64":
            assert type_args is not None
            if not _has_valid_arity(type_args, len(args)) or not _is_integer_in_bounds(args[0], _integer_bounds(type_args, 0)):
                raise ArgumentError("Time64 requires one precision value from 0 through 9")
            return
        elif type_base == "decimal":
            assert type_args is not None
            if not _has_valid_arity(type_args, len(args)) or any(not isinstance(arg, int) for arg in args):
                raise ArgumentError("Decimal requires integer precision and scale arguments")
            precision = args[0]
            scale = args[1]
            assert isinstance(precision, int)
            assert isinstance(scale, int)
            if (
                not _is_integer_in_bounds(precision, _integer_bounds(type_args, 0))
                or not _is_integer_in_bounds(scale, _integer_bounds(type_args, 1))
                or scale > precision
            ):
                raise ArgumentError("Decimal precision and scale are out of range")
            return
        elif type_base in ("decimal32", "decimal64", "decimal128", "decimal256"):
            assert type_args is not None
            bounds = _integer_bounds(type_args, 0)
            _, maximum = bounds
            if not _has_valid_arity(type_args, len(args)) or not _is_integer_in_bounds(args[0], bounds):
                raise ArgumentError(f"{base} requires one scale from 0 through {maximum}")
            return
        elif type_base == "qbit":
            assert type_args is not None
            if (
                not _has_valid_arity(type_args, len(args))
                or args[0] not in ChQBit._ELEMENT_BITS
                or not _is_integer_in_bounds(args[1], _integer_bounds(type_args, 1))
            ):
                raise ArgumentError("QBit requires a supported floating-point type and positive integer dimension")
            return
        elif type_base in ("enum", "enum8", "enum16"):
            assert type_args is not None
            try:
                enum_keys, enum_values = parse_enum(type_name)
            except (IndexError, TypeError, ValueError):
                raise ArgumentError(f"{base} requires one or more name and integer value pairs") from None
            if not enum_values:
                raise ArgumentError(f"{base} requires one or more name and integer value pairs")
            if len(set(enum_keys)) != len(enum_keys) or len(set(enum_values)) != len(enum_values):
                raise ArgumentError(f"{base} names and values must be unique")
            minimum, maximum = _integer_bounds(type_args, "value")
            if any(not _is_integer_in_bounds(value, (minimum, maximum)) for value in enum_values):
                raise ArgumentError(f"{base} values must be from {minimum} through {maximum}")
            return
        elif type_base == "dynamic":
            if opening == -1:
                return
            assert type_args is not None
            if len(args) != type_args.max_args or not isinstance(args[0], str):
                raise ArgumentError("Dynamic accepts one max_types setting")
            setting, separator, value = args[0].partition("=")
            bounds = type_args.bounds_for(setting.strip())
            if separator != "=" or bounds is None:
                raise ArgumentError("Dynamic accepts one max_types setting")
            try:
                max_types = int(value.strip())
            except ValueError:
                raise ArgumentError("Dynamic max_types must be an integer") from None
            minimum, maximum = bounds
            if not _is_integer_in_bounds(max_types, (minimum, maximum)):
                raise ArgumentError(f"Dynamic max_types must be from {minimum} through {maximum}")
            return
        elif type_base == "simpleaggregatefunction":
            assert type_args is not None
            if not _has_valid_arity(type_args, len(args)) or not isinstance(args[0], str) or not args[0]:
                raise ArgumentError("SimpleAggregateFunction requires one function and one result type")
            nested_types = tuple(args[index] for index in type_args.nested_indexes(len(args)))
        elif type_base == "aggregatefunction":
            raise ArgumentError("AggregateFunction is not supported as a JSON typed path")
        elif type_base == "fixedstring":
            assert type_args is not None
            if (
                not _has_valid_arity(type_args, len(args))
                or isinstance(args[0], bool)
                or not _is_integer_in_bounds(args[0], _integer_bounds(type_args, 0))
            ):
                raise ArgumentError("FixedString requires a positive integer size")
            return
        else:
            return

        for nested_type in nested_types:
            if not isinstance(nested_type, str):
                raise ArgumentError(f"Invalid nested ClickHouse type {nested_type!r}")
            JSON._validate_type_expression(nested_type)

    @staticmethod
    def _is_string_literal(value: str) -> bool:
        if len(value) < 2 or value[0] != "'":
            return False
        pos = 1
        while pos < len(value):
            char = value[pos]
            if char == "\\":
                pos += 2
                continue
            if char == "'":
                if pos + 1 < len(value) and value[pos + 1] == "'":
                    pos += 2
                    continue
                return pos == len(value) - 1
            pos += 1
        return False

    @staticmethod
    def _starts_regexp_directive(value: str) -> bool:
        return value[:6].upper() == "REGEXP" and (len(value) == 6 or value[6] == "'" or value[6].isspace())

    @staticmethod
    def _is_quoted_identifier(value: str) -> bool:
        if len(value) < 2 or value[0] not in ('"', "`"):
            return False
        quote = value[0]
        pos = 1
        while pos < len(value):
            char = value[pos]
            if char == "\\":
                pos += 2
                continue
            if char == quote:
                if pos + 1 < len(value) and value[pos + 1] == quote:
                    pos += 2
                    continue
                return pos == len(value) - 1
            pos += 1
        return False

    @staticmethod
    def _is_identifier_expression(value: str) -> bool:
        if not value:
            return False
        pos = 0
        length = len(value)
        first_component = True
        while pos < length:
            char = value[pos]
            if char in ('"', "`"):
                quote = char
                pos += 1
                has_content = False
                while pos < length:
                    char = value[pos]
                    if char == "\\":
                        if pos + 1 >= length:
                            return False
                        has_content = True
                        pos += 2
                    elif char == quote:
                        if pos + 1 < length and value[pos + 1] == quote:
                            has_content = True
                            pos += 2
                        else:
                            pos += 1
                            break
                    else:
                        has_content = True
                        pos += 1
                else:
                    return False
                if not has_content:
                    return False
            else:
                if not ("A" <= char <= "Z" or "a" <= char <= "z" or char == "_"):
                    return False
                start = pos
                pos += 1
                while pos < length:
                    char = value[pos]
                    if not ("A" <= char <= "Z" or "a" <= char <= "z" or "0" <= char <= "9" or char in "_$"):
                        break
                    pos += 1
                if first_component and value[start:pos].upper() == "REGEXP":
                    return False
            first_component = False
            if pos == length:
                return True
            if value[pos] != ".":
                return False
            pos += 1
        return False

    @staticmethod
    def _configured_type_name(type_hint: ChSqlaType) -> str:
        type_name = type_hint.name
        if type_name.lower() == "boolean":
            type_name = "Bool"
        try:
            JSON._validate_type_expression(type_name)
        except ArgumentError as ex:
            raise ArgumentError(f"JSON type {type_hint.__class__.__name__} requires constructor arguments") from ex
        return type_name

    class _Comparator(UserDefinedType.Comparator):  # type: ignore[type-arg]
        """Build storage-backed JSON subcolumn expressions."""

        def __getitem__(self, segment: str) -> ColumnElement[object]:
            return json_subcolumn(self.expr, segment)

        @overload
        def subcolumn(self, segment: str, type_: None = None) -> ColumnElement[object]: ...

        @overload
        def subcolumn(self, segment: str, type_: TypeEngine[_T] | type[TypeEngine[_T]]) -> ColumnElement[_T]: ...

        def subcolumn(
            self,
            segment: str,
            type_: TypeEngine[Any] | type[TypeEngine[Any]] | None = None,
        ) -> ColumnElement[Any]:
            """Select one JSON path segment and optionally cast it."""
            return json_subcolumn(self.expr, segment, type_)

    comparator_factory: type[_Comparator] = _Comparator


class Nested(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    """
    Note this isn't currently supported for insert/select, only table definitions
    """

    python_type = list


class SimpleAggregateFunction(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = str

    def __init__(
        self,
        name: str | None = None,
        element: ChSqlaType | type[ChSqlaType] | None = None,
        type_def: TypeDef | None = None,
    ):
        """
        Constructor that can take the SimpleAggregateFunction name and wrapped type if not constructed from a TypeDef
        :param name: Aggregate function name
        :param element: ChSqlaType instance or class which the function aggregates
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if callable(element):
                element = element()
            if element is None:
                raise ArgumentError("SimpleAggregateFunction requires an element type or type_def")
            type_def = TypeDef(values=(name, element.name))
        super().__init__(type_def)


class AggregateFunction(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    """
    Note this isn't currently supported for insert/select, only table definitions
    """

    python_type = str

    def __init__(self, *params, type_def: TypeDef | None = None):
        """
        Simply wraps the parameters for AggregateFunction for DDL, unless the TypeDef is specified.
        Callables or actual types are converted to their names.
        :param params: AggregateFunction parameters
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            values: tuple[Any, ...] = ()
            for x in params:
                if callable(x):
                    x = x()
                if isinstance(x, ChSqlaType):
                    x = x.name
                values += (x,)
            type_def = TypeDef(values=values)
        super().__init__(type_def)


class QBit(ChSqlaType, UserDefinedType):  # type: ignore[misc]
    python_type = list

    def __init__(self, element_type: str | None = None, dimension: int | None = None, type_def: TypeDef | None = None):
        """
        QBit constructor for bit-transposed vector types
        :param element_type: Element type (BFloat16, Float32, or Float64)
        :param dimension: Number of elements in the vector
        :param type_def: TypeDef from parse_name function (used during reflection)
        """
        if not type_def:
            if not element_type or not dimension:
                raise ArgumentError("QBit requires element_type and dimension parameters")
            type_def = TypeDef(values=(element_type, dimension))
        super().__init__(type_def)


# Static so type checkers can resolve the public surface. test_types.py asserts parity with schema_types.
__all__ = [
    "AggregateFunction",
    "Array",
    "Bool",
    "Boolean",
    "Date",
    "Date32",
    "DateTime",
    "DateTime64",
    "Decimal",
    "Decimal128",
    "Decimal256",
    "Decimal32",
    "Decimal64",
    "Enum",
    "Enum16",
    "Enum8",
    "FixedString",
    "Float32",
    "Float64",
    "Geometry",
    "IPv4",
    "IPv6",
    "Int128",
    "Int16",
    "Int256",
    "Int32",
    "Int64",
    "Int8",
    "JSON",
    "LineString",
    "Map",
    "MultiLineString",
    "MultiPoint",
    "MultiPolygon",
    "Nested",
    "Nothing",
    "Point",
    "Polygon",
    "QBit",
    "Ring",
    "SimpleAggregateFunction",
    "String",
    "Time",
    "Time64",
    "Tuple",
    "UInt128",
    "UInt16",
    "UInt256",
    "UInt32",
    "UInt64",
    "UInt8",
    "UUID",
    "LowCardinality",
    "Nullable",
]
