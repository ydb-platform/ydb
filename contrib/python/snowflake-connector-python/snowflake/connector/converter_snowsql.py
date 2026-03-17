#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from logging import getLogger

import pytz

from .compat import IS_WINDOWS
from .constants import is_date_type_name, is_timestamp_type_name
from .converter import (
    ZERO_EPOCH,
    SnowflakeConverter,
    _adjust_fraction_of_nanoseconds,
    _extract_timestamp,
    _generate_tzinfo_from_tzoffset,
)
from .sfbinaryformat import SnowflakeBinaryFormat, binary_to_python
from .sfdatetime import SnowflakeDateFormat, SnowflakeDateTime, SnowflakeDateTimeFormat

logger = getLogger(__name__)


def format_sftimestamp(ctx, value, franction_of_nanoseconds):
    sf_datetime = SnowflakeDateTime(
        datetime=value, nanosecond=franction_of_nanoseconds, scale=ctx.get("scale")
    )
    return ctx["fmt"].format(sf_datetime) if ctx.get("fmt") else str(sf_datetime)


class SnowflakeConverterSnowSQL(SnowflakeConverter):
    """Snowflake Converter for SnowSQL.

    Format data instead of just converting the values into native
    Python objects.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._support_negative_year = kwargs.get("support_negative_year", True)

    def _get_format(self, type_name):
        """Gets the format."""
        fmt = None
        if type_name == "DATE":
            fmt = self._parameters.get("DATE_OUTPUT_FORMAT")
            if not fmt:
                fmt = "YYYY-MM-DD"
        elif type_name == "TIME":
            fmt = self._parameters.get("TIME_OUTPUT_FORMAT")
        elif type_name + "_OUTPUT_FORMAT" in self._parameters:
            fmt = self._parameters[type_name + "_OUTPUT_FORMAT"]
            if not fmt:
                fmt = self._parameters["TIMESTAMP_OUTPUT_FORMAT"]
        elif type_name == "BINARY":
            fmt = self._parameters.get("BINARY_OUTPUT_FORMAT")
        return fmt

    #
    # FROM Snowflake to Python objects
    #
    def to_python_method(self, type_name, column):
        ctx = column.copy()
        if ctx.get("scale") is not None:
            ctx["max_fraction"] = int(10 ** ctx["scale"])
            ctx["zero_fill"] = "0" * (9 - ctx["scale"])
        fmt = None
        if is_date_type_name(type_name):
            datetime_class = time.struct_time if not IS_WINDOWS else date
            fmt = SnowflakeDateFormat(
                self._get_format(type_name),
                support_negative_year=self._support_negative_year,
                datetime_class=datetime_class,
            )
        elif is_timestamp_type_name(type_name):
            fmt = SnowflakeDateTimeFormat(
                self._get_format(type_name),
                data_type=type_name,
                support_negative_year=self._support_negative_year,
                datetime_class=SnowflakeDateTime,
            )
        elif type_name == "BINARY":
            fmt = SnowflakeBinaryFormat(self._get_format(type_name))
        logger.debug("Type: %s, Format: %s", type_name, fmt)
        ctx["fmt"] = fmt
        converters = [f"_{type_name}_to_python"]
        for conv in converters:
            try:
                return getattr(self, conv)(ctx)
            except AttributeError:
                pass
        logger.warning("No column converter found for type: %s", type_name)
        return None  # Skip conversion

    def _BOOLEAN_to_python(self, ctx):
        """No conversion for SnowSQL."""
        return lambda value: "True" if value in ("1", "True") else "False"

    def _FIXED_to_python(self, ctx):
        """No conversion for SnowSQL."""
        return None

    def _REAL_to_python(self, ctx):
        """No conversion for SnowSQL."""
        return None

    def _BINARY_to_python(self, ctx):
        """BINARY to a string formatted by BINARY_OUTPUT_FORMAT."""
        return lambda value: ctx["fmt"].format(binary_to_python(value))

    def _DATE_to_python(self, ctx):
        """Converts DATE to struct_time/date.

        No timezone is attached.
        """

        def conv(value):
            return ctx["fmt"].format(time.gmtime(int(value) * (24 * 60 * 60)))

        def conv_windows(value):
            ts = ZERO_EPOCH + timedelta(seconds=int(value) * (24 * 60 * 60))
            return ctx["fmt"].format(date(ts.year, ts.month, ts.day))

        return conv if not IS_WINDOWS else conv_windows

    def _TIMESTAMP_TZ_to_python(self, ctx):
        """Converts TIMESTAMP TZ to datetime.

        The timezone offset is piggybacked.
        """
        scale = ctx["scale"]
        max_fraction = ctx.get("max_fraction")

        def conv0(encoded_value):
            value, tz = encoded_value.split()
            microseconds = float(value)
            tzinfo = _generate_tzinfo_from_tzoffset(int(tz) - 1440)
            try:
                t = datetime.fromtimestamp(microseconds, tz=tzinfo)
            except OSError as e:
                logger.debug("OSError occurred but falling back to datetime: %s", e)
                t = ZERO_EPOCH + timedelta(seconds=microseconds)
                if pytz.utc != tzinfo:
                    t += tzinfo.utcoffset(t)
                t = t.replace(tzinfo=tzinfo)
            fraction_of_nanoseconds = _adjust_fraction_of_nanoseconds(
                value, max_fraction, scale
            )

            return format_sftimestamp(ctx, t, fraction_of_nanoseconds)

        def conv(encoded_value):
            value, tz = encoded_value.split()
            microseconds = float(value[0 : -scale + 6])
            tzinfo = _generate_tzinfo_from_tzoffset(int(tz) - 1440)
            try:
                t = datetime.fromtimestamp(microseconds, tz=tzinfo)
            except (OSError, ValueError) as e:
                logger.debug("OSError occurred but falling back to datetime: %s", e)
                t = ZERO_EPOCH + timedelta(seconds=microseconds)
                if pytz.utc != tzinfo:
                    t += tzinfo.utcoffset(t)
                t = t.replace(tzinfo=tzinfo)

            fraction_of_nanoseconds = _adjust_fraction_of_nanoseconds(
                value, max_fraction, scale
            )

            return format_sftimestamp(ctx, t, fraction_of_nanoseconds)

        return conv if scale > 6 else conv0

    def _TIMESTAMP_LTZ_to_python(self, ctx):
        def conv(value):
            t, fraction_of_nanoseconds = self._pre_TIMESTAMP_LTZ_to_python(value, ctx)
            return format_sftimestamp(ctx, t, fraction_of_nanoseconds)

        return conv

    def _TIMESTAMP_NTZ_to_python(self, ctx):
        """Converts TIMESTAMP NTZ to Snowflake Formatted String.

        No timezone info is attached.
        """

        def conv(value):
            microseconds, fraction_of_nanoseconds = _extract_timestamp(value, ctx)
            try:
                t = time.gmtime(microseconds)
            except (OSError, ValueError) as e:
                logger.debug("OSError occurred but falling back to datetime: %s", e)
                t = ZERO_EPOCH + timedelta(seconds=(microseconds))
            return format_sftimestamp(ctx, t, fraction_of_nanoseconds)

        return conv

    _TIME_to_python = _TIMESTAMP_NTZ_to_python
