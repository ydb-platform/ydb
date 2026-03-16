"""This module identifies timezones.

Normally, timezones have ids.
This is a way to access the ids if you have a
datetime.tzinfo object.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, time, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from dateutil.tz import tz

from icalendar.timezone import equivalent_timezone_ids_result
from icalendar.tools import is_date

if TYPE_CHECKING:
    from datetime import datetime, tzinfo

DATEUTIL_UTC = tz.gettz("UTC")
DATEUTIL_UTC_PATH: str | None = getattr(DATEUTIL_UTC, "_filename", None)
DATEUTIL_ZONEINFO_PATH = (
    None if DATEUTIL_UTC_PATH is None else Path(DATEUTIL_UTC_PATH).parent
)


def tzids_from_tzinfo(tzinfo: tzinfo | None) -> tuple[str]:
    """Get several timezone ids if we can identify the timezone.

    >>> import zoneinfo
    >>> from icalendar.timezone.tzid import tzids_from_tzinfo
    >>> tzids_from_tzinfo(zoneinfo.ZoneInfo("Arctic/Longyearbyen"))
    ('Arctic/Longyearbyen', 'Atlantic/Jan_Mayen', 'Europe/Berlin', 'Europe/Budapest', 'Europe/Copenhagen', 'Europe/Oslo', 'Europe/Stockholm', 'Europe/Vienna')
    >>> from dateutil.tz import gettz
    >>> tzids_from_tzinfo(gettz("Europe/Berlin"))
    ('Europe/Berlin', 'Arctic/Longyearbyen', 'Atlantic/Jan_Mayen', 'Europe/Budapest', 'Europe/Copenhagen', 'Europe/Oslo', 'Europe/Stockholm', 'Europe/Vienna')

    """  # The example might need to change if you recreate the lookup tree  # noqa: E501
    if tzinfo is None:
        return ()
    if isinstance(tzinfo, timezone):
        # fixed offset timezone with name
        return get_equivalent_tzids(tzinfo.tzname(None))
    if hasattr(tzinfo, "zone"):
        return get_equivalent_tzids(tzinfo.zone)  # pytz implementation
    if hasattr(tzinfo, "key"):
        return get_equivalent_tzids(tzinfo.key)  # ZoneInfo implementation
    if isinstance(tzinfo, tz._tzicalvtz):  # noqa: SLF001
        return get_equivalent_tzids(tzinfo._tzid)  # noqa: SLF001
    if isinstance(tzinfo, tz.tzstr):
        return get_equivalent_tzids(tzinfo._s)  # noqa: SLF001
    if hasattr(tzinfo, "_filename"):  # dateutil.tz.tzfile  # noqa: SIM102
        if DATEUTIL_ZONEINFO_PATH is not None:
            # tzfile('/usr/share/zoneinfo/Europe/Berlin')
            path = tzinfo._filename  # noqa: SLF001
            if path.startswith(str(DATEUTIL_ZONEINFO_PATH)):
                tzid = str(Path(path).relative_to(DATEUTIL_ZONEINFO_PATH))
                return get_equivalent_tzids(tzid)
            return get_equivalent_tzids(path)
    if isinstance(tzinfo, tz.tzutc):
        return get_equivalent_tzids("UTC")
    return ()


def tzid_from_tzinfo(tzinfo: tzinfo | None) -> str | None:
    """Retrieve the timezone id from the tzinfo object.

    Some timezones are equivalent.
    Thus, we might return one ID that is equivalent to others.
    """
    tzids = tzids_from_tzinfo(tzinfo)
    if "UTC" in tzids:
        return "UTC"
    if not tzids:
        return None
    return tzids[0]


def tzid_from_dt(dt: datetime) -> str | None:
    """Retrieve the timezone id from the datetime object."""
    tzid = tzid_from_tzinfo(dt.tzinfo)
    if tzid is None:
        return dt.tzname()
    return tzid


_EQUIVALENT_IDS: dict[str, set[str]] = defaultdict(set)


def _add_equivalent_ids(value: tuple | dict | set):
    """This adds equivalent ids/

    As soon as one timezone implementation used claims their equivalence,
    they are considered equivalent.
    Have a look at icalendar.timezone.equivalent_timezone_ids.
    """
    if isinstance(value, set):
        for tzid in value:
            _EQUIVALENT_IDS[tzid].update(value)
    elif isinstance(value, tuple):
        _add_equivalent_ids(value[1])
    elif isinstance(value, dict):
        for value2 in value.values():
            _add_equivalent_ids(value2)
    else:
        raise TypeError(
            f"Expected tuple, dict or set, not {value.__class__.__name__}: {value!r}"
        )


_add_equivalent_ids(equivalent_timezone_ids_result.lookup)


def get_equivalent_tzids(tzid: str) -> tuple[str]:
    """This returns the tzids which are equivalent to this one."""
    ids = _EQUIVALENT_IDS.get(tzid, set())
    return (tzid,) + tuple(sorted(ids - {tzid}))


def is_utc(t: datetime | time | date | tzinfo) -> bool:
    """Whether this date is in UTC."""
    if is_date(t):
        return False
    tzid = tzid_from_dt(t) if hasattr(t, "tzinfo") else tzid_from_tzinfo(t)
    return tzid == "UTC"


__all__ = ["is_utc", "tzid_from_dt", "tzid_from_tzinfo", "tzids_from_tzinfo"]
