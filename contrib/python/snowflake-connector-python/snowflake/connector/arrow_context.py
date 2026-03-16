#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import decimal
import time
from datetime import datetime, timedelta
from logging import getLogger

import pytz

from .constants import PARAMETER_TIMEZONE
from .converter import _generate_tzinfo_from_tzoffset

try:
    import numpy
except ImportError:
    numpy = None


try:
    import tzlocal
except ImportError:
    tzlocal = None

ZERO_EPOCH = datetime.utcfromtimestamp(0)

logger = getLogger(__name__)


class ArrowConverterContext:
    """Python helper functions for arrow conversions.

    Windows timestamp functions are necessary because Windows cannot handle -ve timestamps.
    Putting the OS check into the non-windows function would probably take up more CPU cycles then
    just deciding this at compile time.
    """

    def __init__(
        self,
        session_parameters: dict[str, str | int | bool] | None = None,
    ):
        if session_parameters is None:
            session_parameters = {}
        self._timezone = (
            None
            if PARAMETER_TIMEZONE not in session_parameters
            else session_parameters[PARAMETER_TIMEZONE]
        )

    @property
    def timezone(self):
        return self._timezone

    @timezone.setter
    def timezone(self, tz):
        self._timezone = tz

    def _get_session_tz(self):
        """Get the session timezone or use the local computer's timezone."""
        try:
            tz = "UTC" if not self.timezone else self.timezone
            return pytz.timezone(tz)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warning("converting to tzinfo failed")
            if tzlocal is not None:
                return tzlocal.get_localzone()
            else:
                try:
                    return datetime.timezone.utc
                except AttributeError:
                    return pytz.timezone("UTC")

    def TIMESTAMP_TZ_to_python(
        self, epoch: int, microseconds: int, tz: int
    ) -> datetime:
        tzinfo = _generate_tzinfo_from_tzoffset(tz - 1440)
        return datetime.fromtimestamp(epoch, tz=tzinfo) + timedelta(
            microseconds=microseconds
        )

    def TIMESTAMP_TZ_to_python_windows(
        self, epoch: int, microseconds: int, tz: int
    ) -> datetime:
        tzinfo = _generate_tzinfo_from_tzoffset(tz - 1440)
        t = ZERO_EPOCH + timedelta(seconds=epoch, microseconds=microseconds)
        if pytz.utc != tzinfo:
            t += tzinfo.utcoffset(t)
        return t.replace(tzinfo=tzinfo)

    def TIMESTAMP_NTZ_to_python(self, epoch: int, microseconds: int) -> datetime:
        return datetime.utcfromtimestamp(epoch) + timedelta(microseconds=microseconds)

    def TIMESTAMP_NTZ_to_python_windows(
        self, epoch: int, microseconds: int
    ) -> datetime:
        return ZERO_EPOCH + timedelta(seconds=epoch, microseconds=microseconds)

    def TIMESTAMP_LTZ_to_python(self, epoch: int, microseconds: int) -> datetime:
        tzinfo = self._get_session_tz()
        return datetime.fromtimestamp(epoch, tz=tzinfo) + timedelta(
            microseconds=microseconds
        )

    def TIMESTAMP_LTZ_to_python_windows(
        self, epoch: int, microseconds: int
    ) -> datetime:
        try:
            tzinfo = self._get_session_tz()
            ts = ZERO_EPOCH + timedelta(seconds=epoch, microseconds=microseconds)
            return pytz.utc.localize(ts, is_dst=False).astimezone(tzinfo)
        except OverflowError:
            logger.debug(
                "OverflowError in converting from epoch time to "
                "timestamp_ltz: %s(ms). Falling back to use struct_time."
            )
            return time.localtime(microseconds)

    def REAL_to_numpy_float64(self, py_double):
        return numpy.float64(py_double)

    def FIXED_to_numpy_int64(self, py_long):
        return numpy.int64(py_long)

    def FIXED_to_numpy_float64(self, py_long, scale):
        return numpy.float64(decimal.Decimal(py_long).scaleb(-scale))

    def DATE_to_numpy_datetime64(self, py_days):
        return numpy.datetime64(py_days, "D")

    def TIMESTAMP_NTZ_ONE_FIELD_to_numpy_datetime64(self, value, scale):
        nanoseconds = int(decimal.Decimal(value).scaleb(9 - scale))
        return numpy.datetime64(nanoseconds, "ns")

    def TIMESTAMP_NTZ_TWO_FIELD_to_numpy_datetime64(self, epoch, fraction):
        nanoseconds = int(decimal.Decimal(epoch).scaleb(9) + decimal.Decimal(fraction))
        return numpy.datetime64(nanoseconds, "ns")
