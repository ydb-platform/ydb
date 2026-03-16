# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

import datetime
import re

UTC = datetime.timezone.utc


def parse_timestamp(value: str) -> datetime.datetime:
    r"""
    Parse a timestamp.

    This can either use use "YYYY-MM-DD hh:mm:ss" or epoch notation.

    For future compatibility, the timestamp may start
    with an alphanumeric prefix (think: URL-scheme)
    that forces a particular parsing.

    Implemented prefixes (case-insensitive):

    * `epoch:` or `@` to parse the number as an Unix epoch
    * `rfc3339:` to get the "ISO" format, with the following changes:
      A timezone offset is optional.
      The "T" separator may be replaced by a space.

    Examples of parsing epochs:

    >>> parse_timestamp("1640606727").isoformat()  # no prefix
    '2021-12-27T12:05:27+00:00'
    >>> parse_timestamp("@1640606727").isoformat()
    '2021-12-27T12:05:27+00:00'
    >>> parse_timestamp("epoch:1640606727").isoformat()
    '2021-12-27T12:05:27+00:00'

    Examples of parsing RFC 3339 like timestamps:

    >>> parse_timestamp("2021-12-27 13:05:27").isoformat()  # no prefix
    '2021-12-27T13:05:27'
    >>> parse_timestamp("rfc3339:2021-12-27 13:05:27").isoformat()
    '2021-12-27T13:05:27'

    Examples of invalid formats:

    >>> parse_timestamp("illegal-scheme:foo")
    Traceback (most recent call last):
      ...
    ValueError: unknown timestamp format
    >>> parse_timestamp("tomorrow at 2 PM")
    Traceback (most recent call last):
      ...
    ValueError: unknown timestamp format
    """

    if value.startswith("@"):
        return _parse_epoch(value[1:])

    # handle explicit schemes
    scheme_match = re.fullmatch(r"([a-zA-Z][a-zA-Z0-9+.-]*):(.*)", value)
    if scheme_match is not None:
        scheme = scheme_match.group(1).lower()
        value = scheme_match.group(2)
        if scheme == "epoch":
            return _parse_epoch(value)
        if scheme == "rfc3339":
            return _parse_rfc3339(value)
        raise ValueError("unknown timestamp format")

    # guess the format
    for parser in [_parse_epoch, _parse_rfc3339]:
        try:
            return parser(value)
        except ValueError:
            pass

    raise ValueError("unknown timestamp format")


def _parse_epoch(value: str) -> datetime.datetime:
    r"""
    Parse an epoch timestamp.

    Epoch timestamps are always resolved in the UTC timezone.

    >>> _parse_epoch("1640606727").isoformat()
    '2021-12-27T12:05:27+00:00'
    >>> _parse_epoch("invalid")
    Traceback (most recent call last):
      ...
    ValueError: not a valid Unix epoch
    """
    try:
        return datetime.datetime.fromtimestamp(int(value), UTC)
    except Exception:
        raise ValueError("not a valid Unix epoch") from None


def _parse_rfc3339(value: str) -> datetime.datetime:
    r"""
    Parse an RFC-3339 or ISO-like timestamp.

    This will accept timestamps in the following formats:

    * `YYYY-MMM-DD hh:mm:ss`
    * same, but with `T` separator instead of space
    * same, but with timezone offset (e.g. `+hh:mm` or `Z`)

    Note that timestamps without an explicit timezone are naive
    and represent a local time,
    which may complicate some aspects of testing.

    Examples for separators:

    >>> _parse_rfc3339("2021-12-27 13:05:27").isoformat()
    '2021-12-27T13:05:27'
    >>> _parse_rfc3339("2021-12-27T13:05:27").isoformat()
    '2021-12-27T13:05:27'
    >>> _parse_rfc3339("2021-12-27t13:05:27").isoformat()
    '2021-12-27T13:05:27'
    >>> _parse_rfc3339("2021-12-27@13:05:27+12:30")
    Traceback (most recent call last):
      ...
    ValueError: timestamp separator must be 'T' or space

    Examples for timezone offsets:

    >>> _parse_rfc3339("2021-12-27 13:05:27Z").isoformat()
    '2021-12-27T13:05:27+00:00'
    >>> _parse_rfc3339("2021-12-27 13:05:27z").isoformat()
    '2021-12-27T13:05:27+00:00'
    >>> _parse_rfc3339("2021-12-27 13:05:27+12:30").isoformat()
    '2021-12-27T13:05:27+12:30'
    >>> _parse_rfc3339("2021-12-27T13:05:27-07:23").isoformat()
    '2021-12-27T13:05:27-07:23'

    Examples for invalid syntax:

    >>> _parse_rfc3339("2021/12/27 13:05:27")
    Traceback (most recent call last):
      ...
    ValueError: timestamp must use RFC-3339 ...

    >>> _parse_rfc3339("2021-12-27 13:05:27 UTC")
    Traceback (most recent call last):
      ...
    ValueError: timezone offset must be 'Z' or +hh:mm

    >>> _parse_rfc3339("test")
    Traceback (most recent call last):
      ...
    ValueError: timestamp must use RFC-3339 ...
    """

    err_must_use_rfc_3339 = "timestamp must use RFC-3339 (YYYY-MM-DD hh:mm:ss) format"

    if len(value) < 19:
        raise ValueError(err_must_use_rfc_3339)

    date_value = value[:10]  # YYYY-MM-DD
    sep = value[10]  # T or space
    time_value = value[11:19]  # hh:mm:ss
    tz_value = value[19:]  # empty or Z or +hh:mm

    if sep.lower() not in ("t", " "):
        raise ValueError("timestamp separator must be 'T' or space")

    try:
        naive_timestamp = datetime.datetime.strptime(
            date_value + " " + time_value,
            "%Y-%m-%d %H:%M:%S",
        )
    except ValueError:
        raise ValueError(err_must_use_rfc_3339) from None

    if not tz_value:
        return naive_timestamp

    timezone = _parse_timezone(tz_value)
    return naive_timestamp.replace(tzinfo=timezone)


def _parse_timezone(value: str) -> datetime.timezone:
    r"""
    Unfortunately, it is necessary to handle timezones manually.
    Python's supported strptime format specifiers are not sufficient.
    For example, "%z" does not match "Z" on all Python versions
    and might not support "+hh:mm" style timezone offsets.
    """

    if value.lower() == "z":
        value = "+00:00"

    tz_match = re.fullmatch(r"([+-])([0-9]{2}):([0-9]{2})", value)
    if tz_match is None:
        raise ValueError("timezone offset must be 'Z' or +hh:mm")
    sign, hours, minutes = tz_match.groups()
    offset_sign = +1 if sign == "+" else -1
    offset_hours = int(hours)
    offset_minutes = int(minutes)

    offset = offset_sign * datetime.timedelta(
        hours=offset_hours,
        minutes=offset_minutes,
    )
    if offset.total_seconds() == 0:
        return UTC
    return datetime.timezone(offset)
