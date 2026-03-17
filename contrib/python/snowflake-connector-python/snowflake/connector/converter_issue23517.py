#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from datetime import datetime, time, timedelta, tzinfo
from functools import partial
from logging import getLogger

import pytz

from .converter import ZERO_EPOCH, SnowflakeConverter, _generate_tzinfo_from_tzoffset

logger = getLogger(__name__)


class SnowflakeConverterIssue23517(SnowflakeConverter):
    """Converter for Python 3.5.0 or Any Python on Windows.

    This is to address http://bugs.python.org/issue23517
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logger.debug("initialized")

    def _TIMESTAMP_TZ_to_python(self, ctx):
        scale = ctx["scale"]

        def conv(encoded_value: str) -> datetime:
            value, tz = encoded_value.split()
            tzinfo = _generate_tzinfo_from_tzoffset(int(tz) - 1440)
            return SnowflakeConverterIssue23517.create_timestamp_from_string(
                value=value, scale=scale, tz=tzinfo
            )

        return conv

    def _TIMESTAMP_LTZ_to_python(self, ctx):
        tzinfo = self._get_session_tz()
        scale = ctx["scale"]

        def conv(value: str) -> datetime:
            ts = SnowflakeConverterIssue23517.create_timestamp_from_string(
                value=value, scale=scale
            )
            return pytz.utc.localize(ts, is_dst=False).astimezone(tzinfo)

        return conv

    def _TIMESTAMP_NTZ_to_python(self, ctx):
        scale = ctx["scale"]
        return partial(
            SnowflakeConverterIssue23517.create_timestamp_from_string, scale=scale
        )

    def _TIME_to_python(self, ctx):
        """Converts TIME to formatted string, SnowflakeDateTime, or datetime.time.

        No timezone is attached.
        """
        scale = ctx["scale"]

        def conv0(value: str) -> time:
            return (ZERO_EPOCH + timedelta(seconds=(float(value)))).time()

        def conv(value: str) -> time:
            microseconds = float(value[0 : -scale + 6])
            return (ZERO_EPOCH + timedelta(seconds=(microseconds))).time()

        return conv if scale > 6 else conv0

    @staticmethod
    def create_timestamp_from_string(
        value: str,
        scale: int,
        tz: tzinfo | None = None,
    ) -> datetime:
        """Windows does not support negative timestamps, so we need to do that part in Python."""
        seconds, fraction = SnowflakeConverter.get_seconds_microseconds(
            value=value, scale=scale
        )
        if not tz:
            return datetime.utcfromtimestamp(0) + timedelta(
                seconds=seconds, microseconds=fraction
            )
        return datetime.fromtimestamp(0, tz=tz) + timedelta(
            seconds=seconds, microseconds=fraction
        )
