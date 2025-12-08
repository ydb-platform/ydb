import pytz

import array
from datetime import date, datetime, tzinfo, timedelta, time

from typing import Union, Sequence, MutableSequence, Any, NamedTuple, Optional
from abc import abstractmethod
import re

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType
from clickhouse_connect.common import get_setting
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.common import write_array, np_date_types, int_size, first_value
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.ctypes import data_conv, numpy_conv
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.options import np, pd, IS_PANDAS_2

epoch_start_date = date(1970, 1, 1)
epoch_start_datetime = datetime(1970, 1, 1)


class Date(ClickHouseType):
    _array_type = 'H'
    np_type = 'datetime64[D]'
    nano_divisor = 86400 * 1000000000
    valid_formats = 'native', 'int'
    python_type = date
    byte_size = 2

    @property
    def pandas_dtype(self):
        if IS_PANDAS_2 and get_setting("preserve_pandas_datetime_resolution"):
            return "datetime64[s]"
        return f"datetime64[{self.pd_datetime_res}]"

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state:Any):
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        if ctx.use_numpy:
            return numpy_conv.read_numpy_array(source, '<u2', num_rows).astype(self.np_type)
        return data_conv.read_date_col(source, num_rows)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            if isinstance(first, datetime):
                esd = epoch_start_datetime
            else:
                esd = epoch_start_date
            if self.nullable:
                column = [0 if x is None else (x - esd).days for x in column]
            else:
                column = [(x - esd).days for x in column]
        write_array(self._array_type, column, dest, ctx.column_name)

    def _active_null(self, ctx: QueryContext):
        fmt = self.read_format(ctx)
        if ctx.use_extended_dtypes:
            return pd.NA if fmt == 'int' else pd.NaT
        if ctx.use_none:
            return None
        if fmt == 'int':
            return 0
        if ctx.use_numpy:
            return np.datetime64(0, self.pd_datetime_res)
        return epoch_start_date

    # pylint: disable=too-many-return-statements
    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if self.read_format(ctx) == 'int':
            return column

        if ctx.use_numpy and self.nullable and not ctx.use_none:
            return np.array(column, dtype=self.np_type)

        if ctx.use_extended_dtypes:
            if isinstance(column, np.ndarray) and np.issubdtype(
                column.dtype, np.datetime64
            ):
                return column.astype(self.pandas_dtype)

            if isinstance(column, pd.DatetimeIndex):
                if column.tz is None:
                    return column.astype(self.pandas_dtype)

                naive = column.tz_convert("UTC").tz_localize(None).astype(self.pandas_dtype)
                return naive.tz_localize("UTC").tz_convert(column.tz)

            if self.nullable and isinstance(column, list):
                return np.array([None if pd.isna(s) else s for s in column]).astype(
                    self.pandas_dtype
                )

            return pd.to_datetime(column, errors="coerce").to_numpy(
                dtype=self.pandas_dtype, copy=False
            )

        return column


class Date32(Date):
    byte_size = 4
    _array_type = 'l' if int_size == 2 else 'i'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        if ctx.use_numpy:
            return numpy_conv.read_numpy_array(source, '<i4', num_rows).astype(self.np_type)
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_date32_col(source, num_rows)


class DateTimeBase(ClickHouseType, registered=False):
    __slots__ = ('tzinfo',)
    valid_formats = 'native', 'int'
    python_type = datetime

    @property
    def pandas_dtype(self):
        """Sets dtype for pandas datetime objects"""
        if IS_PANDAS_2 and get_setting("preserve_pandas_datetime_resolution"):
            return "datetime64[s]"
        return f"datetime64[{self.pd_datetime_res}]"

    def _active_null(self, ctx: QueryContext):
        fmt = self.read_format(ctx)
        if ctx.use_extended_dtypes:
            return pd.NA if fmt == 'int' else pd.NaT
        if ctx.use_none:
            return None
        if self.read_format(ctx) == 'int':
            return 0
        if ctx.use_numpy:
            return np.datetime64(0, self.pd_datetime_res)
        return epoch_start_datetime

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        """Ensure every datetime-like column is at nanosecond resolution, preserving any tz."""
        if ctx.use_extended_dtypes:
            if isinstance(column, np.ndarray) and np.issubdtype(
                column.dtype, np.datetime64
            ):
                return column.astype(self.pandas_dtype)

            if isinstance(column, pd.DatetimeIndex) or (
                isinstance(column, list)
                and hasattr(next((s for s in column if not pd.isna(s)), None), "tz")
            ):
                if isinstance(column, list):
                    column = pd.DatetimeIndex(column)

                if column.tz is None:
                    result = column.astype(self.pandas_dtype)
                    return pd.array(result) if self.nullable else result

                naive_ns = column.tz_convert("UTC").tz_localize(None).astype(self.pandas_dtype)
                tz_aware_result = naive_ns.tz_localize("UTC").tz_convert(column.tz)
                return (
                    pd.array(tz_aware_result) if self.nullable else tz_aware_result
                )

            if self.nullable:
                return pd.array(
                    [None if pd.isna(s) else s for s in column], dtype=self.pandas_dtype
                )
        return column


class DateTime(DateTimeBase):
    _array_type = 'L' if int_size == 2 else 'I'
    np_type = 'datetime64[s]'
    nano_divisor = 1000000000
    byte_size = 4

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        if len(type_def.values) > 0:
            self.tzinfo = pytz.timezone(type_def.values[0][1:-1])
        else:
            self.tzinfo = None

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any) -> Sequence:
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        active_tz = ctx.active_tz(self.tzinfo)
        if ctx.use_numpy:
            np_array = numpy_conv.read_numpy_array(source, '<u4', num_rows).astype(self.np_type)
            if ctx.as_pandas and active_tz:
                return pd.DatetimeIndex(np_array, tz='UTC').tz_convert(active_tz)
            return np_array
        return data_conv.read_datetime_col(source, num_rows, active_tz)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            if self.nullable:
                column = [int(x.timestamp()) if x else 0 for x in column]
            else:
                column = [int(x.timestamp()) for x in column]
        write_array(self._array_type, column, dest, ctx.column_name)


class DateTime64(DateTimeBase):
    __slots__ = 'scale', 'prec', 'unit'
    byte_size = 8

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.scale = type_def.values[0]
        self.prec = 10 ** self.scale
        self.unit = np_date_types.get(self.scale)
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
        else:
            self.tzinfo = None

    @property
    def pandas_dtype(self):
        """Sets dtype for pandas datetime objects"""
        if IS_PANDAS_2 and get_setting("preserve_pandas_datetime_resolution"):
            return f"datetime64{self.unit}"
        return f"datetime64[{self.pd_datetime_res}]"

    @property
    def np_type(self):
        if self.unit:
            return f'datetime64{self.unit}'
        raise ProgrammingError(f'Cannot use {self.name} as a numpy or Pandas datatype. Only milliseconds(3), ' +
                               'microseconds(6), or nanoseconds(9) are supported for numpy based queries.')

    @property
    def nano_divisor(self):
        return 1000000000 // self.prec

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any) -> Sequence:
        if self.read_format(ctx) == 'int':
            return source.read_array('q', num_rows)
        active_tz = ctx.active_tz(self.tzinfo)
        if ctx.use_numpy:
            np_array = numpy_conv.read_numpy_array(source, self.np_type, num_rows)
            if ctx.as_pandas and active_tz:
                return pd.DatetimeIndex(np_array, tz='UTC').tz_convert(active_tz)
            return np_array
        column = source.read_array('q', num_rows)
        if active_tz:
            return self._read_binary_tz(column, active_tz)
        return self._read_binary_naive(column)

    def _read_binary_tz(self, column: Sequence, tz_info: tzinfo):
        new_col = []
        app = new_col.append
        dt_from = datetime.fromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds, tz_info)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _read_binary_naive(self, column: Sequence):
        new_col = []
        app = new_col.append
        dt_from = tzutil.utcfromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        elif isinstance(first, str):
            original_column = column
            column = []

            for x in original_column:
                if not x and self.nullable:
                    v = 0
                else:
                    dt = datetime.fromisoformat(x)
                    v = ((int(dt.timestamp()) * 1000000 + dt.microsecond) * self.prec) // 1000000

                column.append(v)
        else:
            prec = self.prec
            if self.nullable:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 if x else 0
                          for x in column]
            else:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
        write_array('q', column, dest, ctx.column_name)


class _HMSParts(NamedTuple):
    """Internal structure for parsed HMS time components."""

    hours: int
    minutes: int
    seconds: int
    frac: Optional[str]
    is_negative: bool


class TimeBase(ClickHouseType, registered=False):
    """
    Abstract base for ClickHouse Time and Time64 types.

    Subclasses must define:
      - _array_type: Array type specifier (e.g. 'i' or 'q')
      - byte_size: Size in bytes for binary representation
      - np_type: NumPy array type (e.g. 'timedelta64[s]' or 'timedelta64[ns]')

    And implement these abstract methods:
      - _string_to_ticks(self, str) -> int
      - _timedelta_to_ticks(self, timedelta) -> int
      - _ticks_to_timedelta(self, int) -> timedelta
      - _ticks_to_string(self, int) -> str
      - max_ticks and min_ticks properties
    """

    _HMS_RE = re.compile(
        r"""^\s*
        (?P<sign>-?)
        (?P<hours>\d+):
        (?P<minutes>\d+):
        (?P<seconds>\d+)
        (?:\.(?P<frac>\d+))?
        \s*$""",
        re.VERBOSE,
    )

    MAX_TIME_SECONDS = 999 * 3600 + 59 * 60 + 59  # 999:59:59
    MIN_TIME_SECONDS = -MAX_TIME_SECONDS  # -999:59:59
    _MICROS_PER_SECOND = 1_000_000
    _NANOS_PER_SECOND = 1_000_000_000
    _SECONDS_PER_DAY = 86_400

    _array_type: str
    byte_size: int
    np_type: str
    valid_formats = ("native", "string", "int", "time")
    python_type = timedelta

    def _read_column_binary(
        self,
        source: ByteSource,
        num_rows: int,
        ctx: QueryContext,
        _read_state: Any,
    ) -> Sequence:
        """Read binary column data and convert to requested format."""
        ticks = source.read_array(self._array_type, num_rows)
        fmt = self.read_format(ctx)

        if ctx.use_numpy:
            return np.array(
                [self._ticks_to_np_timedelta(t) for t in ticks], dtype=self.np_type
            )

        if fmt == "int":
            return ticks

        if fmt == "string":
            return [self._ticks_to_string(t) for t in ticks]

        if fmt == "time":
            return [self._ticks_to_time(t) for t in ticks]

        return [self._ticks_to_timedelta(t) for t in ticks]

    def _write_column_binary(
        self,
        column: Sequence,
        dest: bytearray,
        ctx: InsertContext,
    ):
        """Write column data in binary format."""
        ticks = self._to_ticks_array(column)
        write_array(self._array_type, ticks, dest, ctx.column_name)

    def _parse_core(self, time_str: str) -> _HMSParts:
        """Parse an hhh:mm:ss[.fff] time literal."""
        match = self._HMS_RE.match(time_str)
        if not match:
            raise ValueError(f"Invalid time literal {time_str}")

        hours = int(match["hours"])
        minutes = int(match["minutes"])
        seconds = int(match["seconds"])

        if hours > 999:
            raise ValueError(
                f"Hours out of range; cannot exceed 999: got {hours} in '{time_str}'"
            )
        if not 0 <= minutes < 60:
            raise ValueError(
                f"Minutes out of range; must be 0-59: got {minutes} in '{time_str}'"
            )
        if not 0 <= seconds < 60:
            raise ValueError(
                f"Seconds out of range; must be 0-59: got {seconds} in '{time_str}'"
            )

        return _HMSParts(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            frac=match["frac"],
            is_negative=bool(match["sign"]),
        )

    def _to_ticks_array(self, column: Sequence) -> Sequence[int]:
        """Convert column data to internal tick representation."""
        first = first_value(column, self.nullable)
        expected_type = type(first) if first is not None else None

        if expected_type is None:
            if self.nullable:
                return [0] * len(column)
            return []

        converter_map = {
            timedelta: self._timedelta_to_ticks,
            time: self._time_to_ticks,
            float: self._numerical_to_ticks,
            int: self._numerical_to_ticks,
            str: self._string_to_ticks,
        }
        if np is not None:
            converter_map[np.timedelta64] = self._timedelta_to_ticks
            converter_map[np.int64] = self._numerical_to_ticks
        converter = converter_map.get(expected_type, None)

        if converter is None:
            raise TypeError(
                f"Unsupported column type '{expected_type.__name__}' for {self.__class__.__name__}. "
                "Expected 'int', 'str', 'time', or 'timedelta'."
            )

        if self.nullable:
            return [converter(x) if x is not None else 0 for x in column]

        return [converter(x) for x in column]

    def _validate_standard_range(self, ticks: int, original: Any) -> None:
        """Validate that ticks is within valid ClickHouse range."""
        if not self.min_ticks <= ticks <= self.max_ticks:
            raise ValueError(f"{original} out of range for {self.__class__.__name__}")

    def _validate_time_obj_range(self, ticks: int) -> None:
        """Ensure ticks can form a valid datetime.time object."""
        if not self.min_time_ticks <= ticks <= self.max_time_ticks:
            raise ValueError(
                f"Ticks value {ticks} is outside valid range for datetime.time object."
            )

    def _numerical_to_ticks(self, value: Union[int, float, "np.int64"]) -> int:
        """Convert numerical value to ticks, with range validation."""
        value = int(value)
        self._validate_standard_range(value, value)
        return value

    def _active_null(self, ctx: QueryContext):
        """Return appropriate null value based on context."""
        fmt = self.read_format(ctx)
        if ctx.use_extended_dtypes:
            return pd.NA if fmt == "int" else pd.NaT
        if ctx.use_none:
            return None
        if fmt == "int":
            return 0
        if fmt == "string":
            return "00:00:00"
        if ctx.use_numpy:
            return np.timedelta64("NaT")

        return timedelta(0)

    @property
    def pandas_dtype(self):
        """Sets dtype for pandas datetime objects"""
        if IS_PANDAS_2 and get_setting("preserve_pandas_datetime_resolution"):
            return "timedelta64[s]"
        return f"timedelta64[{self.pd_datetime_res}]"

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        """Finalize column data based on context requirements."""
        if ctx.use_extended_dtypes:
            if isinstance(column, np.ndarray) and np.issubdtype(
                column.dtype, np.timedelta64
            ):
                return column.astype(self.pandas_dtype)

            if isinstance(column, pd.TimedeltaIndex):
                return column.astype(self.pandas_dtype)

            if self.nullable:
                return np.array([None if pd.isna(s) else s for s in column]).astype(
                    self.pandas_dtype
                )
        return column

    def _build_lc_column(self, index: Sequence, keys: array.array, ctx: QueryContext):
        """Build low-cardinality column from index and keys."""
        if ctx.use_numpy:
            return np.array([index[k] for k in keys], dtype=self.np_type)

        return super()._build_lc_column(index, keys, ctx)

    @abstractmethod
    def _string_to_ticks(self, time_str: str) -> int:
        """Parse a string into integer ticks."""
        raise NotImplementedError

    @abstractmethod
    def _timedelta_to_ticks(self, td: Union[timedelta, "np.timedelta64"]) -> int:
        """Convert a timedelta into integer ticks."""
        raise NotImplementedError

    @abstractmethod
    def _ticks_to_time(self, ticks: int) -> time:
        """Convert integer ticks into a time."""
        raise NotImplementedError

    @abstractmethod
    def _time_to_ticks(self, t: time) -> int:
        """Convert a time into integer ticks."""
        raise NotImplementedError

    @abstractmethod
    def _ticks_to_timedelta(self, ticks: int) -> timedelta:
        """Convert integer ticks into a timedelta."""
        raise NotImplementedError

    @abstractmethod
    def _ticks_to_np_timedelta(self, ticks: int) -> timedelta:
        """Convert integer ticks into an np.timedelta."""
        raise NotImplementedError

    @abstractmethod
    def _ticks_to_string(self, ticks: int) -> str:
        """Format integer ticks as a string."""
        raise NotImplementedError

    @property
    def min_time_ticks(self) -> int:
        """Minimum tick value representable by datetime.time type."""
        return 0

    @property
    @abstractmethod
    def max_time_ticks(self) -> int:
        """Maximum tick value representable by datetime.time type."""
        raise NotImplementedError

    @property
    @abstractmethod
    def max_ticks(self) -> int:
        """Maximum tick value representable by this type."""
        raise NotImplementedError

    @property
    @abstractmethod
    def min_ticks(self) -> int:
        """Minimum tick value representable by this type."""
        raise NotImplementedError


class Time(TimeBase):
    """ClickHouse Time type with second precision."""

    _array_type = "i"
    byte_size = 4
    np_type = "timedelta64[s]"

    @property
    def max_ticks(self) -> int:
        return self.MAX_TIME_SECONDS

    @property
    def min_ticks(self) -> int:
        return self.MIN_TIME_SECONDS

    @property
    def max_time_ticks(self) -> int:
        return self._SECONDS_PER_DAY - 1

    def _string_to_ticks(self, time_str: str) -> int:
        """Parse string format 'HHH:MM:SS[.fff]' to ticks (seconds), flooring fractional seconds."""
        parts = self._parse_core(time_str)
        ticks = parts.hours * 3600 + parts.minutes * 60 + parts.seconds

        if parts.is_negative:
            ticks = -ticks
        self._validate_standard_range(ticks, time_str)

        return ticks

    def _ticks_to_string(self, ticks: int) -> str:
        """Format ticks (seconds) as 'HHH:MM:SS' string."""
        sign = "-" if ticks < 0 else ""
        t = abs(ticks)
        h, rem = divmod(t, 3600)
        m, s = divmod(rem, 60)

        return f"{sign}{h:03d}:{m:02d}:{s:02d}"

    def _timedelta_to_ticks(self, td: Union[timedelta, "np.timedelta64"]) -> int:
        """Convert timedelta to ticks (seconds), flooring fractional seconds."""
        if isinstance(td, timedelta):
            total = int(td.total_seconds())
        else:
            total = td.astype("timedelta64[s]").astype(int)
        self._validate_standard_range(total, td)

        return total

    def _ticks_to_timedelta(self, ticks: int) -> timedelta:
        """Convert ticks (seconds) to timedelta."""
        return timedelta(seconds=ticks)

    def _ticks_to_np_timedelta(self, ticks: int) -> timedelta:
        """Convert ticks (seconds) to np.timedelta."""
        return np.timedelta64(ticks, "s")

    def _time_to_ticks(self, t: time) -> int:
        """Converts time to ticks (seconds), flooring fraction seconds."""
        return t.hour * 3600 + t.minute * 60 + t.second

    def _ticks_to_time(self, ticks: int) -> time:
        """Converts ticks (seconds) to time."""
        self._validate_time_obj_range(ticks)
        h, rem = divmod(ticks, 3600)
        m, s = divmod(rem, 60)

        return time(hour=h, minute=m, second=s)


class Time64(TimeBase):
    """ClickHouse Time64 type with configurable sub-second precision."""

    __slots__ = ("scale", "precision", "unit")
    _array_type = "q"
    byte_size = 8

    def __init__(self, type_def):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.scale = type_def.values[0]
        if self.scale not in (3, 6, 9):
            raise ProgrammingError(
                f"Unsupported Time64 scale {self.scale}; "
                "only 3, 6, or 9 are allowed for NumPy."
            )
        self.precision = 10**self.scale
        self.unit = np_date_types.get(self.scale)

    @property
    def pandas_dtype(self):
        """Sets dtype for pandas datetime objects"""
        if IS_PANDAS_2 and get_setting("preserve_pandas_datetime_resolution"):
            return f"timedelta64{self.unit}"
        return f"timedelta64[{self.pd_datetime_res}]"

    @property
    def max_time_ticks(self) -> int:
        return self._SECONDS_PER_DAY * self.precision - 1

    @property
    def np_type(self) -> str:
        return f"timedelta64{self.unit}"

    @property
    def max_ticks(self) -> int:
        return self.MAX_TIME_SECONDS * self.precision + (self.precision - 1)

    @property
    def min_ticks(self) -> int:
        return -self.max_ticks

    def _string_to_ticks(self, time_str: str) -> int:
        """Parse string format 'HHH:MM:SS[.fff]' to ticks with sub-second precision."""
        parts = self._parse_core(time_str)
        frac_ticks = int((parts.frac or "").ljust(self.scale, "0")[: self.scale])
        ticks = (
            parts.hours * 3600 + parts.minutes * 60 + parts.seconds
        ) * self.precision + frac_ticks
        if parts.is_negative:
            ticks = -ticks
        self._validate_standard_range(ticks, time_str)

        return ticks

    def _ticks_to_string(self, ticks: int) -> str:
        """Format ticks as 'HHH:MM:SS[.fff]' string with sub-second precision."""
        sign = "-" if ticks < 0 else ""
        t = abs(ticks)
        sec_part, frac_part = divmod(t, self.precision)
        h, rem = divmod(sec_part, 3600)
        m, s = divmod(rem, 60)
        frac_str = f".{frac_part:0{self.scale}d}" if self.scale else ""

        return f"{sign}{h:03d}:{m:02d}:{s:02d}{frac_str}"

    def _timedelta_to_ticks(self, td: Union[timedelta, "np.timedelta64"]) -> int:
        """Convert timedelta to ticks with sub-second precision."""
        if isinstance(td, timedelta):
            total_us = (
                int(td.total_seconds()) * self._MICROS_PER_SECOND + td.microseconds
            )
            ticks = (total_us * self.precision) // self._MICROS_PER_SECOND
        else:
            ticks = td.astype("timedelta64[s]").astype(int)
        self._validate_standard_range(ticks, td)

        return ticks

    def _ticks_to_timedelta(self, ticks: int) -> timedelta:
        """Convert ticks to timedelta with microsecond precision."""
        neg = ticks < 0
        t = abs(ticks)
        sec_part = t // self.precision
        frac_part = t - sec_part * self.precision
        micros = (frac_part * self._MICROS_PER_SECOND) // self.precision
        td = timedelta(seconds=sec_part, microseconds=micros)

        return -td if neg else td

    def _ticks_to_np_timedelta(self, ticks: int) -> "np.timedelta64":
        """Convert ticks to numpy timedelta64 with nanosecond precision."""
        res_map = {3: "ms", 6: "us", 9: "ns"}

        return np.timedelta64(ticks, res_map.get(self.scale))

    def _time_to_ticks(self, t: time) -> int:
        """Convert time to ticks with sub-second precision."""
        total_us = (
            t.hour * 3600 + t.minute * 60 + t.second
        ) * self._MICROS_PER_SECOND + t.microsecond
        ticks = (total_us * self.precision) // self._MICROS_PER_SECOND
        self._validate_time_obj_range(ticks)

        return ticks

    def _ticks_to_time(self, ticks: int) -> time:
        """Convert ticks to time with microsecond precision."""
        self._validate_time_obj_range(ticks)
        sec_part, frac_part = divmod(ticks, self.precision)
        h, rem = divmod(sec_part, 3600)
        m, s = divmod(rem, 60)
        micros = (frac_part * self._MICROS_PER_SECOND) // self.precision

        return time(hour=h, minute=m, second=s, microsecond=micros)
