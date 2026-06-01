import os
import zoneinfo
from datetime import datetime, timezone, tzinfo

tzlocal = None
try:
    import tzlocal  # Maybe we can use the tzlocal module to get a safe timezone
except ImportError:
    pass

# Set the local timezone for DateTime conversions.  Note in most cases we want to use either UTC or the server
# timezone, but if someone insists on using the local timezone we will try to convert.  The problem is we
# never have anything but an epoch timestamp returned from ClickHouse, so attempts to convert times when the
# local timezone is "DST" aware (like 'CEST' vs 'CET') will be wrong approximately half the time
local_tz: tzinfo
local_tz_dst_safe: bool = False

# Zero-offset IANA timezone aliases that are semantically UTC.  Listing every alias lets
# resolve_zone() short-circuit these names without needing a system zoneinfo database, matching
# the behavior pytz provided by bundling its own tz data.
UTC_EQUIVALENTS = (
    "UTC",
    "Etc/UTC",
    "UCT",
    "Etc/UCT",
    "GMT",
    "Etc/GMT",
    "GMT0",
    "GMT-0",
    "GMT+0",
    "Etc/GMT0",
    "Etc/GMT-0",
    "Etc/GMT+0",
    "Universal",
    "Etc/Universal",
    "Zulu",
    "Etc/Zulu",
    "Greenwich",
    "Etc/Greenwich",
)

# Appended to error/warning messages when a named IANA zone cannot be resolved. On systems without
# a system zoneinfo database (slim containers, Windows without tzdata), users can install the tzdata
# extra to get the IANA zone data.
TZDATA_HINT = "install the tzdata package (e.g. `pip install clickhouse-connect[tzdata]`) if no system zoneinfo database is available"


def resolve_zone(tz_name: str) -> tzinfo:
    """Resolve an IANA timezone name to a tzinfo.

    Short-circuits UTC-equivalent names to datetime.timezone.utc so that representing UTC
    does not require an IANA zoneinfo database to be available on the host. Other names are
    resolved via zoneinfo.ZoneInfo and will raise ZoneInfoNotFoundError if the host has
    no system zoneinfo and the tzdata package is not installed.
    """
    if tz_name in UTC_EQUIVALENTS:
        return timezone.utc
    try:
        return zoneinfo.ZoneInfo(tz_name)
    except ValueError as ex:
        # ZoneInfo raises ValueError for empty strings, absolute paths, and non-normalized
        # keys; funnel those into ZoneInfoNotFoundError so callers only need one except clause.
        raise zoneinfo.ZoneInfoNotFoundError(str(ex)) from ex


def normalize_timezone(tz: tzinfo) -> tuple[tzinfo, bool]:
    # ZoneInfo exposes the IANA key on `.key`; fall back to tzname(None) for other tzinfo
    # subclasses (datetime.timezone, fixed offsets). pytz used to return the IANA name from
    # tzname(None), but ZoneInfo returns None, which would collapse every named zone into the
    # "unsafe" fallback branch.
    tz_key = getattr(tz, "key", None) or tz.tzname(None)

    if tz_key in UTC_EQUIVALENTS:
        return timezone.utc, True

    if tz_key in zoneinfo.available_timezones():
        return tz, True

    if tzlocal is not None:  # Maybe we can use the tzlocal module to get a safe timezone
        local_name = tzlocal.get_localzone_name()
        if local_name in zoneinfo.available_timezones():
            return zoneinfo.ZoneInfo(local_name), True

    return tz, False


def is_utc_timezone(tz: tzinfo | str | None) -> bool:
    """Check if timezone is UTC or an equivalent (Etc/UTC, GMT, etc.).

    This handles the issue where zoneinfo.ZoneInfo('Etc/UTC') != zoneinfo.ZoneInfo("UTC") despite
    being semantically equivalent. Also accepts timezone name strings.
    """
    if tz is None:
        return False
    if isinstance(tz, str):
        return tz in UTC_EQUIVALENTS
    if tz is timezone.utc:
        return True
    return tz.tzname(None) in UTC_EQUIVALENTS


def utc_equivalent_tzaware_datetime(ts: int, microseconds: int, tz_info: tzinfo) -> datetime:
    """Build a UTC-equivalent timezone-aware datetime via epoch arithmetic.

    For UTC-equivalent timezones (UTC, Etc/UTC, GMT, etc.), construct the datetime
    using epoch arithmetic rather than datetime.fromtimestamp(), then attach the
    timezone. This avoids timezone conversion machinery that's unnecessary for UTC.

    Sub-second precision must be supplied via the microseconds argument; the ts
    value is interpreted as integer seconds.

    Args:
        ts: Integer Unix timestamp (seconds since epoch)
        microseconds: Microsecond component (0-999999)
        tz_info: A UTC-equivalent timezone object

    Returns:
        Timezone-aware datetime in the specified timezone
    """
    seconds = int(ts)

    days = seconds // 86400
    secs_in_day = seconds % 86400

    year, month, day = _epoch_days_to_date_components(days)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime(year, month, day, hour, minute, second, microseconds, tzinfo=tz_info)


def utcfromtimestamp_with_microseconds(ts: int, microseconds: int = 0) -> datetime:
    """Convert integer Unix timestamp to naive UTC datetime with explicit microseconds.

    More efficient than calling utcfromtimestamp() and then .replace(microsecond=...)
    because it constructs the datetime once with all components.

    Args:
        ts: Integer Unix timestamp (seconds since epoch)
        microseconds: Microsecond component (0-999999)

    Returns:
        Naive UTC datetime with specified microseconds
    """
    seconds = int(ts)

    days = seconds // 86400
    secs_in_day = seconds % 86400

    year, month, day = _epoch_days_to_date_components(days)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime(year, month, day, hour, minute, second, microseconds)


def utcfromtimestamp(ts: int) -> datetime:
    """Convert integer Unix timestamp to naive UTC datetime via epoch arithmetic.

    Avoids the expensive datetime.fromtimestamp() + replace() round-trip. Sub-second
    precision is not supported; pass an integer number of seconds. For sub-second
    inputs, use utcfromtimestamp_with_microseconds.
    """
    seconds = int(ts)

    days = seconds // 86400
    secs_in_day = seconds % 86400

    year, month, day = _epoch_days_to_date_components(days)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime(year, month, day, hour, minute, second, 0)


_MONTH_DAYS = (0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365)
_MONTH_DAYS_LEAP = (0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366)


def _epoch_days_to_date_components(days: int) -> tuple[int, int, int]:
    """Convert days since epoch to (year, month, day).

    This is a pure Python implementation of the same algorithm as
    the Cython epoch_days_to_date, but returns components instead of a date object.
    """
    if 0 <= days < 47482:
        cycles = (days + 365) // 1461
        rem = (days + 365) - cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + years + 1969
        if years == 4:
            return year - 1, 12, 31
        if years == 3:
            m_list = _MONTH_DAYS_LEAP
        else:
            m_list = _MONTH_DAYS
    else:
        cycles400 = (days + 134774) // 146097
        rem = days + 134774 - (cycles400 * 146097)
        cycles100 = rem // 36524
        rem -= cycles100 * 36524
        cycles = rem // 1461
        rem -= cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + cycles400 * 400 + cycles100 * 100 + years + 1601
        if years == 4 or cycles100 == 4:
            return year - 1, 12, 31
        if years == 3 and year % 100 != 0:
            m_list = _MONTH_DAYS_LEAP
        else:
            m_list = _MONTH_DAYS

    month = (rem + 24) >> 5
    prev = m_list[month]
    while rem < prev:
        month -= 1
        prev = m_list[month]

    return year, month + 1, rem + 1 - prev


def _detect_local_tz() -> tzinfo:
    env_tz = os.environ.get("TZ")
    if env_tz:
        try:
            return resolve_zone(env_tz)
        except zoneinfo.ZoneInfoNotFoundError:
            pass
    return datetime.now().astimezone().tzinfo


local_tz, local_tz_dst_safe = normalize_timezone(_detect_local_tz())
