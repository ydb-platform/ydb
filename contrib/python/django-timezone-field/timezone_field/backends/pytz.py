import pytz

from .base import TimeZoneBackend, TimeZoneNotFoundError


class PYTZBackend(TimeZoneBackend):
    utc_tzobj = pytz.utc
    all_tzstrs = pytz.all_timezones
    base_tzstrs = pytz.common_timezones

    def is_tzobj(self, value):
        return value is pytz.UTC or isinstance(value, pytz.tzinfo.BaseTzInfo)

    def to_tzobj(self, tzstr):
        try:
            return pytz.timezone(tzstr)
        except pytz.UnknownTimeZoneError as err:
            raise TimeZoneNotFoundError from err
