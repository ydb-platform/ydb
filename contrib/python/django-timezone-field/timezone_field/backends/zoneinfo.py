try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from .base import TimeZoneBackend, TimeZoneNotFoundError


class ZoneInfoBackend(TimeZoneBackend):
    utc_tzobj = zoneinfo.ZoneInfo("UTC")
    all_tzstrs = zoneinfo.available_timezones()
    base_tzstrs = zoneinfo.available_timezones()
    # Remove the "Factory" timezone as it can cause ValueError exceptions on
    # some systems, e.g. FreeBSD, if the system zoneinfo database is used.
    all_tzstrs.discard("Factory")
    base_tzstrs.discard("Factory")

    def is_tzobj(self, value):
        return isinstance(value, zoneinfo.ZoneInfo)

    def to_tzobj(self, tzstr):
        if tzstr in (None, ""):
            raise TimeZoneNotFoundError
        try:
            return zoneinfo.ZoneInfo(tzstr)
        except zoneinfo.ZoneInfoNotFoundError as err:
            raise TimeZoneNotFoundError from err
