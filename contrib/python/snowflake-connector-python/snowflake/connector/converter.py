#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import binascii
import decimal
import time
from datetime import date, datetime
from datetime import time as dt_t
from datetime import timedelta, tzinfo
from functools import partial
from logging import getLogger
from math import ceil
from typing import Any, Callable

import pytz

from .compat import IS_BINARY, IS_NUMERIC
from .errorcode import ER_NOT_SUPPORT_DATA_TYPE
from .errors import ProgrammingError
from .sfbinaryformat import binary_to_python, binary_to_snowflake
from .sfdatetime import sfdatetime_total_seconds_from_timedelta

try:
    import numpy
except ImportError:
    numpy = None
try:
    import tzlocal
except ImportError:
    tzlocal = None

BITS_FOR_TIMEZONE = 14
ZERO_TIMEDELTA = timedelta(seconds=0)
ZERO_EPOCH_DATE = date(1970, 1, 1)
ZERO_EPOCH = datetime.utcfromtimestamp(0)
ZERO_FILL = "000000000"

logger = getLogger(__name__)

PYTHON_TO_SNOWFLAKE_TYPE = {
    "int": "FIXED",
    "long": "FIXED",
    "decimal": "FIXED",
    "float": "REAL",
    "str": "TEXT",
    "unicode": "TEXT",
    "bytes": "BINARY",
    "bytearray": "BINARY",
    "bool": "BOOLEAN",
    "bool_": "BOOLEAN",
    "nonetype": "ANY",
    "datetime": "TIMESTAMP_NTZ",
    "sfdatetime": "TIMESTAMP_NTZ",
    "date": "DATE",
    "time": "TIME",
    "struct_time": "TIMESTAMP_NTZ",
    "timedelta": "TIME",
    "list": "TEXT",
    "tuple": "TEXT",
    "int8": "FIXED",
    "int16": "FIXED",
    "int32": "FIXED",
    "int64": "FIXED",
    "uint8": "FIXED",
    "uint16": "FIXED",
    "uint32": "FIXED",
    "uint64": "FIXED",
    "float16": "REAL",
    "float32": "REAL",
    "float64": "REAL",
    "datetime64": "TIMESTAMP_NTZ",
    "quoted_name": "TEXT",
}

# Type alias
SnowflakeConverterType = Callable[[Any], Any]


def convert_datetime_to_epoch(dt: datetime) -> float:
    """Converts datetime to epoch time in seconds.

    If Python > 3.3, you may use timestamp() method.
    """
    if dt.tzinfo is not None:
        dt0 = dt.astimezone(pytz.UTC).replace(tzinfo=None)
    else:
        dt0 = dt
    return (dt0 - ZERO_EPOCH).total_seconds()


def _convert_datetime_to_epoch_nanoseconds(dt: datetime) -> str:
    return f"{convert_datetime_to_epoch(dt):f}".replace(".", "") + "000"


def _convert_date_to_epoch_milliseconds(dt: datetime) -> str:
    return f"{(dt - ZERO_EPOCH_DATE).total_seconds():.3f}".replace(".", "")


def _convert_time_to_epoch_nanoseconds(tm: dt_t) -> str:
    return (
        str(tm.hour * 3600 + tm.minute * 60 + tm.second)
        + f"{tm.microsecond:06d}"
        + "000"
    )


def _extract_timestamp(value: str, ctx: dict) -> tuple[float, int]:
    """Extracts timestamp from a raw data."""
    scale = ctx["scale"]
    microseconds = float(value[0 : -scale + 6]) if scale > 6 else float(value)
    fraction_of_nanoseconds = _adjust_fraction_of_nanoseconds(
        value, ctx["max_fraction"], scale
    )

    return microseconds, fraction_of_nanoseconds


def _adjust_fraction_of_nanoseconds(value: str, max_fraction: int, scale: int) -> int:
    if scale == 0:
        return 0
    if value[0] != "-":
        return int(value[-scale:] + ZERO_FILL[: 9 - scale])

    frac = int(value[-scale:])
    if frac == 0:
        return 0
    else:
        return int(str(max_fraction - frac) + ZERO_FILL[: 9 - scale])


def _generate_tzinfo_from_tzoffset(tzoffset_minutes: int) -> tzinfo:
    """Generates tzinfo object from tzoffset."""
    return pytz.FixedOffset(tzoffset_minutes)


class SnowflakeConverter:
    def __init__(self, **kwargs):
        self._parameters: dict[str, str | int | bool] = {}
        self._use_numpy = kwargs.get("use_numpy", False) and numpy is not None

        logger.debug("use_numpy: %s", self._use_numpy)

    def set_parameters(self, new_parameters: dict) -> None:
        self._parameters = new_parameters

    def set_parameter(self, param: Any, value: Any) -> None:
        self._parameters[param] = value

    def get_parameters(self) -> dict[str, str | int | bool]:
        return self._parameters

    def get_parameter(self, param: str) -> str | int | bool | None:
        return self._parameters.get(param)

    def to_python_method(self, type_name, column) -> SnowflakeConverterType:
        """FROM Snowflake to Python Objects"""
        ctx = column.copy()
        if ctx.get("scale") is not None:
            ctx["max_fraction"] = int(10 ** ctx["scale"])
            ctx["zero_fill"] = "0" * (9 - ctx["scale"])
        converters = [f"_{type_name}_to_python"]
        if self._use_numpy:
            converters.insert(0, f"_{type_name}_numpy_to_python")
        for conv in converters:
            try:
                return getattr(self, conv)(ctx)
            except AttributeError:
                pass
        logger.warning("No column converter found for type: %s", type_name)
        return None  # Skip conversion

    def _FIXED_to_python(self, ctx):
        return int if ctx["scale"] == 0 else decimal.Decimal

    def _FIXED_numpy_to_python(self, ctx):
        if ctx["scale"]:
            return numpy.float64
        else:

            def conv(value):
                try:
                    return numpy.int64(value)
                except OverflowError:
                    return int(value)

            return conv

    def _REAL_to_python(self, _):
        return float

    def _REAL_numpy_to_python(self, _):
        return numpy.float64

    def _TEXT_to_python(self, _):
        return None  # skip conv

    def _BINARY_to_python(self, _):
        return binary_to_python

    def _DATE_to_python(self, _):
        """Converts DATE to date."""

        def conv(value: str) -> date:
            try:
                return datetime.utcfromtimestamp(int(value) * 86400).date()
            except (OSError, ValueError) as e:
                logger.debug("Failed to convert: %s", e)
                ts = ZERO_EPOCH + timedelta(seconds=int(value) * (24 * 60 * 60))
                return date(ts.year, ts.month, ts.day)

        return conv

    def _DATE_numpy_to_python(self, _):
        """Converts DATE to datetime.

        No timezone is attached.
        """
        return lambda x: numpy.datetime64(int(x), "D")

    def _TIMESTAMP_TZ_to_python(self, ctx):
        """Converts TIMESTAMP TZ to datetime.

        The timezone offset is piggybacked.
        """
        scale = ctx["scale"]

        def conv(encoded_value: str) -> datetime:
            value, tz = encoded_value.split()
            tzinfo = _generate_tzinfo_from_tzoffset(int(tz) - 1440)
            return SnowflakeConverter.create_timestamp_from_string(
                value=value, scale=scale, tz=tzinfo
            )

        return conv

    def _get_session_tz(self):
        """Gets the session timezone or use the local computer's timezone."""
        try:
            tz = self.get_parameter("TIMEZONE")
            if not tz:
                tz = "UTC"
            return pytz.timezone(tz)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warning("converting to tzinfo failed")
            if tzlocal is not None:
                return tzlocal.get_localzone()
            else:
                return datetime.timezone.utc

    def _pre_TIMESTAMP_LTZ_to_python(self, value, ctx) -> datetime:
        """Converts TIMESTAMP LTZ to datetime.

        This takes consideration of the session parameter TIMEZONE if available. If not, tzlocal is used.
        """
        microseconds, fraction_of_nanoseconds = _extract_timestamp(value, ctx)
        tzinfo_value = self._get_session_tz()

        try:
            t0 = ZERO_EPOCH + timedelta(seconds=microseconds)
            t = pytz.utc.localize(t0, is_dst=False).astimezone(tzinfo_value)
            return t, fraction_of_nanoseconds
        except OverflowError:
            logger.debug(
                "OverflowError in converting from epoch time to "
                "timestamp_ltz: %s(ms). Falling back to use struct_time."
            )
            return time.localtime(microseconds), fraction_of_nanoseconds

    def _TIMESTAMP_LTZ_to_python(self, ctx):
        tzinfo = self._get_session_tz()
        scale = ctx["scale"]

        return partial(
            SnowflakeConverter.create_timestamp_from_string, scale=scale, tz=tzinfo
        )

    _TIMESTAMP_to_python = _TIMESTAMP_LTZ_to_python

    def _TIMESTAMP_NTZ_to_python(self, ctx):
        """TIMESTAMP NTZ to datetime with no timezone info is attached."""
        scale = ctx["scale"]

        return partial(SnowflakeConverter.create_timestamp_from_string, scale=scale)

    def _TIMESTAMP_NTZ_numpy_to_python(self, ctx):
        """TIMESTAMP NTZ to datetime64 with no timezone info is attached."""

        def conv(value: str) -> numpy.datetime64:
            nanoseconds = int(decimal.Decimal(value).scaleb(9))
            return numpy.datetime64(nanoseconds, "ns")

        return conv

    def _TIME_to_python(self, ctx):
        """TIME to formatted string, SnowflakeDateTime, or datetime.time with no timezone attached."""
        scale = ctx["scale"]

        def conv0(value):
            return datetime.utcfromtimestamp(float(value)).time()

        def conv(value: str) -> dt_t:
            microseconds = float(value[0 : -scale + 6])
            return datetime.utcfromtimestamp(microseconds).time()

        return conv if scale > 6 else conv0

    def _VARIANT_to_python(self, _):
        return None  # skip conv

    _OBJECT_to_python = _VARIANT_to_python

    _ARRAY_to_python = _VARIANT_to_python

    def _BOOLEAN_to_python(self, ctx):
        return lambda value: value in ("1", "TRUE")

    def snowflake_type(self, value):
        """Returns Snowflake data type for the value. This is used for qmark parameter style."""
        type_name = value.__class__.__name__.lower()
        return PYTHON_TO_SNOWFLAKE_TYPE.get(type_name)

    def to_snowflake_bindings(self, snowflake_type, value):
        """Converts Python data to snowflake data for qmark and numeric parameter style.

        The output is bound in a query in the server side.
        """
        type_name = value.__class__.__name__.lower()
        return getattr(self, f"_{type_name}_to_snowflake_bindings")(
            snowflake_type, value
        )

    def _str_to_snowflake_bindings(self, _, value: str) -> str:
        # NOTE: str type is always taken as a text data and never binary
        return str(value)

    _int_to_snowflake_bindings = _str_to_snowflake_bindings
    _long_to_snowflake_bindings = _str_to_snowflake_bindings
    _float_to_snowflake_bindings = _str_to_snowflake_bindings
    _unicode_to_snowflake_bindings = _str_to_snowflake_bindings
    _decimal_to_snowflake_bindings = _str_to_snowflake_bindings

    def _bytes_to_snowflake_bindings(self, _, value: bytes) -> str:
        return binascii.hexlify(value).decode("utf-8")

    _bytearray_to_snowflake_bindings = _bytes_to_snowflake_bindings

    def _bool_to_snowflake_bindings(self, _, value: bool) -> str:
        return str(value).lower()

    def _nonetype_to_snowflake_bindings(self, *_) -> None:
        return None

    def _date_to_snowflake_bindings(self, _, value: date) -> str:
        # milliseconds
        return _convert_date_to_epoch_milliseconds(value)

    def _time_to_snowflake_bindings(self, _, value: dt_t) -> str:
        # nanoseconds
        return _convert_time_to_epoch_nanoseconds(value)

    def _datetime_to_snowflake_bindings(
        self, snowflake_type: str, value: datetime
    ) -> str:
        snowflake_type = snowflake_type.upper()
        if snowflake_type == "TIMESTAMP_LTZ":
            _, t = self._derive_offset_timestamp(value)
            return _convert_datetime_to_epoch_nanoseconds(t)
        elif snowflake_type == "TIMESTAMP_NTZ":
            # nanoseconds
            return _convert_datetime_to_epoch_nanoseconds(value)
        elif snowflake_type == "TIMESTAMP_TZ":
            offset, t = self._derive_offset_timestamp(value, is_utc=True)
            return _convert_datetime_to_epoch_nanoseconds(t) + " {:04d}".format(
                int(offset)
            )
        else:
            raise ProgrammingError(
                msg="Binding datetime object with Snowflake data type {} is "
                "not supported.".format(snowflake_type),
                errno=ER_NOT_SUPPORT_DATA_TYPE,
            )

    def _derive_offset_timestamp(
        self, value: datetime, is_utc: bool = False
    ) -> tuple[float, datetime]:
        """Derives TZ offset and timestamp from the datetime objects."""
        tzinfo = value.tzinfo
        if tzinfo is None:
            # If no tzinfo is attached, use local timezone.
            tzinfo = self._get_session_tz() if not is_utc else pytz.UTC
            t = pytz.utc.localize(value, is_dst=False).astimezone(tzinfo)
        else:
            # if tzinfo is attached, just covert to epoch time
            # as the server expects it in UTC anyway
            t = value
        offset = tzinfo.utcoffset(t.replace(tzinfo=None)).total_seconds() / 60 + 1440
        return offset, t

    def _struct_time_to_snowflake_bindings(
        self, snowflake_type: str, value: time.struct_time
    ) -> str:
        return self._datetime_to_snowflake_bindings(
            snowflake_type, datetime.fromtimestamp(time.mktime(value))
        )

    def _timedelta_to_snowflake_bindings(
        self, snowflake_type: str, value: timedelta
    ) -> str:
        snowflake_type = snowflake_type.upper()
        if snowflake_type != "TIME":
            raise ProgrammingError(
                msg="Binding timedelta object with Snowflake data type {} is "
                "not supported.".format(snowflake_type),
                errno=ER_NOT_SUPPORT_DATA_TYPE,
            )
        (hours, r) = divmod(value.seconds, 3600)
        (mins, secs) = divmod(r, 60)
        hours += value.days * 24
        return (
            str(hours * 3600 + mins * 60 + secs) + f"{value.microseconds:06d}" + "000"
        )

    def to_snowflake(self, value: Any) -> Any:
        """Converts Python data to Snowflake data for pyformat/format style.

        The output is bound in a query in the client side.
        """
        type_name = value.__class__.__name__.lower()
        return getattr(self, f"_{type_name}_to_snowflake")(value)

    def _int_to_snowflake(self, value: int) -> int:
        return int(value)

    def _long_to_snowflake(self, value):
        return long(value)

    def _float_to_snowflake(self, value: float) -> float:
        return float(value)

    def _str_to_snowflake(self, value: str) -> str:
        return str(value)

    _unicode_to_snowflake = _str_to_snowflake

    def _bytes_to_snowflake(self, value: bytes) -> bytes:
        return binary_to_snowflake(value)

    _bytearray_to_snowflake = _bytes_to_snowflake

    def _bool_to_snowflake(self, value: bool) -> bool:
        return value

    def _bool__to_snowflake(self, value) -> bool:
        return bool(value)

    def _nonetype_to_snowflake(self, _):
        return None

    def _total_seconds_from_timedelta(self, td: timedelta) -> int:
        return sfdatetime_total_seconds_from_timedelta(td)

    def _datetime_to_snowflake(self, value: datetime) -> str:
        tzinfo_value = value.tzinfo
        if tzinfo_value:
            if pytz.utc != tzinfo_value:
                try:
                    td = tzinfo_value.utcoffset(value)
                except pytz.exceptions.AmbiguousTimeError:
                    td = tzinfo_value.utcoffset(value, is_dst=False)
            else:
                td = ZERO_TIMEDELTA
            sign = "+" if td >= ZERO_TIMEDELTA else "-"
            td_secs = sfdatetime_total_seconds_from_timedelta(td)
            h, m = divmod(abs(td_secs // 60), 60)
            if value.microsecond:
                return (
                    "{year:d}-{month:02d}-{day:02d} "
                    "{hour:02d}:{minute:02d}:{second:02d}."
                    "{microsecond:06d}{sign}{tzh:02d}:{tzm:02d}"
                ).format(
                    year=value.year,
                    month=value.month,
                    day=value.day,
                    hour=value.hour,
                    minute=value.minute,
                    second=value.second,
                    microsecond=value.microsecond,
                    sign=sign,
                    tzh=h,
                    tzm=m,
                )
            return (
                "{year:d}-{month:02d}-{day:02d} "
                "{hour:02d}:{minute:02d}:{second:02d}"
                "{sign}{tzh:02d}:{tzm:02d}"
            ).format(
                year=value.year,
                month=value.month,
                day=value.day,
                hour=value.hour,
                minute=value.minute,
                second=value.second,
                sign=sign,
                tzh=h,
                tzm=m,
            )
        else:
            if value.microsecond:
                return (
                    "{year:d}-{month:02d}-{day:02d} "
                    "{hour:02d}:{minute:02d}:{second:02d}."
                    "{microsecond:06d}"
                ).format(
                    year=value.year,
                    month=value.month,
                    day=value.day,
                    hour=value.hour,
                    minute=value.minute,
                    second=value.second,
                    microsecond=value.microsecond,
                )
            return (
                "{year:d}-{month:02d}-{day:02d} " "{hour:02d}:{minute:02d}:{second:02d}"
            ).format(
                year=value.year,
                month=value.month,
                day=value.day,
                hour=value.hour,
                minute=value.minute,
                second=value.second,
            )

    def _date_to_snowflake(self, value: date) -> str:
        """Converts Date object to Snowflake object."""
        return "{year:d}-{month:02d}-{day:02d}".format(
            year=value.year, month=value.month, day=value.day
        )

    def _time_to_snowflake(self, value: dt_t) -> str:
        if value.microsecond:
            return value.strftime("%H:%M:%S.%%06d") % value.microsecond
        return value.strftime("%H:%M:%S")

    def _struct_time_to_snowflake(self, value: time.struct_time) -> str:
        tzinfo_value = _generate_tzinfo_from_tzoffset(time.timezone // 60)
        t = datetime.fromtimestamp(time.mktime(value))
        if pytz.utc != tzinfo_value:
            t += tzinfo_value.utcoffset(t)
        t = t.replace(tzinfo=tzinfo_value)
        return self._datetime_to_snowflake(t)

    def _timedelta_to_snowflake(self, value: timedelta) -> str:
        (hours, r) = divmod(value.seconds, 3600)
        (mins, secs) = divmod(r, 60)
        hours += value.days * 24
        if value.microseconds:
            return ("{hour:02d}:{minute:02d}:{second:02d}." "{microsecond:06d}").format(
                hour=hours, minute=mins, second=secs, microsecond=value.microseconds
            )
        return "{hour:02d}:{minute:02d}:{second:02d}".format(
            hour=hours, minute=mins, second=secs
        )

    def _decimal_to_snowflake(self, value: decimal.Decimal) -> str | None:
        if isinstance(value, decimal.Decimal):
            return str(value)

        return None

    def _list_to_snowflake(self, value: list) -> list:
        return [
            SnowflakeConverter.quote(v0)
            for v0 in [SnowflakeConverter.escape(v) for v in value]
        ]

    _tuple_to_snowflake = _list_to_snowflake

    def __numpy_to_snowflake(self, value):
        return value

    _int8_to_snowflake = __numpy_to_snowflake
    _int16_to_snowflake = __numpy_to_snowflake
    _int32_to_snowflake = __numpy_to_snowflake
    _int64_to_snowflake = __numpy_to_snowflake
    _uint8_to_snowflake = __numpy_to_snowflake
    _uint16_to_snowflake = __numpy_to_snowflake
    _uint32_to_snowflake = __numpy_to_snowflake
    _uint64_to_snowflake = __numpy_to_snowflake
    _float16_to_snowflake = __numpy_to_snowflake
    _float32_to_snowflake = __numpy_to_snowflake
    _float64_to_snowflake = __numpy_to_snowflake

    def _datetime64_to_snowflake(self, value) -> str:
        return str(value) + "+00:00"

    def _quoted_name_to_snowflake(self, value) -> str:
        return str(value)

    def __getattr__(self, item):
        if item.endswith("_to_snowflake"):
            raise ProgrammingError(
                msg="Binding data in type ({}) is not supported.".format(
                    item[1 : item.find("_to_snowflake")]
                ),
                errno=ER_NOT_SUPPORT_DATA_TYPE,
            )
        elif item.endswith("to_snowflake_bindings"):
            raise ProgrammingError(
                msg="Binding data in type ({}) is not supported.".format(
                    item[1 : item.find("_to_snowflake_bindings")]
                ),
                errno=ER_NOT_SUPPORT_DATA_TYPE,
            )
        raise AttributeError(f"No method is available: {item}")

    def to_csv_bindings(self, value: tuple[str, Any] | Any) -> str | None:
        """Convert value to a string representation in CSV-escaped format to INSERT INTO."""
        if isinstance(value, tuple) and len(value) == 2:
            _type, val = value
            if _type in ["TIMESTAMP_TZ", "TIME"]:
                # unspecified timezone is considered utc
                if getattr(val, "tzinfo", 1) is None:
                    val = self.to_snowflake(pytz.utc.localize(val))
                else:
                    val = self.to_snowflake(val)
            else:
                val = self.to_snowflake_bindings(_type, val)
        else:
            if isinstance(value, (dt_t, timedelta)):
                val = self.to_snowflake(value)
            else:
                _type = self.snowflake_type(value)
                val = self.to_snowflake_bindings(_type, value)
        return self.escape_for_csv(val)

    @staticmethod
    def escape(value):
        if isinstance(value, list):
            return value
        if value is None or IS_NUMERIC(value) or IS_BINARY(value):
            return value
        res = value
        res = res.replace("\\", "\\\\")
        res = res.replace("\n", "\\n")
        res = res.replace("\r", "\\r")
        res = res.replace("\047", "\134\047")  # single quotes
        return res

    @staticmethod
    def quote(value) -> str:
        if isinstance(value, list):
            return ",".join(value)
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif IS_NUMERIC(value):
            return str(repr(value))
        elif IS_BINARY(value):
            # Binary literal syntax
            return "X'{}'".format(value.decode("ascii"))

        return f"'{value}'"

    @staticmethod
    def escape_for_csv(value: str) -> str:
        if value is None:  # NULL
            return ""
        elif not value:  # Empty string
            return '""'
        if (
            value.find('"') >= 0
            or value.find("\n") >= 0
            or value.find(",") >= 0
            or value.find("\\") >= 0
        ):
            # replace single quote with double quotes
            value = value.replace('"', '""')
            return f'"{value}"'
        else:
            return value

    @staticmethod
    def get_seconds_microseconds(
        value: str,
        scale: int,
    ) -> tuple[int, int]:
        """Calculate the second and microsecond parts og a timestamp given as a string.

        The trick is that we always want to do floor division, but if the timestamp
        is negative then it is given as its inverse. So -0.000_000_009
        (which is 1969-12-31-23:59:59.999999991) should round down to 6
        fraction figures as Python doesn't support sub-microseconds.
        Ultimately for the aforementioned example we should return two integers 0 and -000_001.
        """
        negative = value[0] == "-"
        lhs, _, rhs = value.partition(".")
        seconds = int(lhs)
        microseconds = int(rhs) if rhs else 0
        if scale < 6:
            microseconds *= 10 ** (6 - scale)
        elif scale > 6:
            if negative:
                microseconds = ceil(microseconds / 10 ** (scale - 6))
            else:
                microseconds = microseconds // 10 ** (scale - 6)
        if negative:
            microseconds = -microseconds
        return seconds, microseconds

    @staticmethod
    def create_timestamp_from_string(
        value: str,
        scale: int,
        tz: tzinfo | None = None,
    ) -> datetime:
        seconds, fraction = SnowflakeConverter.get_seconds_microseconds(
            value=value, scale=scale
        )
        if not tz:
            return datetime.utcfromtimestamp(seconds) + timedelta(microseconds=fraction)
        return datetime.fromtimestamp(seconds, tz=tz) + timedelta(microseconds=fraction)
