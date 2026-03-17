from __future__ import annotations

from typing import TYPE_CHECKING

from icalendar.tools import to_datetime

from .windows_to_olson import WINDOWS_TO_OLSON

if TYPE_CHECKING:
    import datetime

    from dateutil.rrule import rrule

    from icalendar import prop
    from icalendar.cal import Timezone

    from .provider import TZProvider

DEFAULT_TIMEZONE_PROVIDER = "zoneinfo"


class TZP:
    """This is the timezone provider proxy.

    If you would like to have another timezone implementation,
    you can create a new one and pass it to this proxy.
    All of icalendar will then use this timezone implementation.
    """

    def __init__(self, provider: str | TZProvider = DEFAULT_TIMEZONE_PROVIDER):
        """Create a new timezone implementation proxy."""
        self.use(provider)

    def use_pytz(self) -> None:
        """Use pytz as the timezone provider."""
        from .pytz import PYTZ  # noqa: PLC0415, RUF100

        self._use(PYTZ())

    def use_zoneinfo(self) -> None:
        """Use zoneinfo as the timezone provider."""
        from .zoneinfo import ZONEINFO  # noqa: PLC0415, RUF100

        self._use(ZONEINFO())

    def _use(self, provider: TZProvider) -> None:
        """Use a timezone implementation."""
        self.__tz_cache = {}
        self.__provider = provider

    def use(self, provider: str | TZProvider):
        """Switch to a different timezone provider."""
        if isinstance(provider, str):
            use_provider = getattr(self, f"use_{provider}", None)
            if use_provider is None:
                raise ValueError(
                    f"Unknown provider {provider}. Use 'pytz' or 'zoneinfo'."
                )
            use_provider()
        else:
            self._use(provider)

    def use_default(self):
        """Use the default timezone provider."""
        self.use(DEFAULT_TIMEZONE_PROVIDER)

    def localize_utc(self, dt: datetime.date) -> datetime.datetime:
        """Return the datetime in UTC.

        If the datetime has no timezone, set UTC as its timezone.
        """
        return self.__provider.localize_utc(to_datetime(dt))

    def localize(
        self, dt: datetime.date, tz: datetime.tzinfo | str | None
    ) -> datetime.datetime:
        """Localize a datetime to a timezone."""
        if isinstance(tz, str):
            tz = self.timezone(tz)
        if tz is None:
            return dt.replace(tzinfo=None)
        return self.__provider.localize(to_datetime(dt), tz)

    def cache_timezone_component(self, timezone_component: Timezone.Timezone) -> None:
        """Cache the timezone that is created from a timezone component
        if it is not already known.

        This can influence the result from timezone(): Once cached, the
        custom timezone is returned from timezone().
        """
        _unclean_id = timezone_component["TZID"]
        _id = self.clean_timezone_id(_unclean_id)
        if (
            not self.__provider.knows_timezone_id(_id)
            and not self.__provider.knows_timezone_id(_unclean_id)
            and _id not in self.__tz_cache
        ):
            self.__tz_cache[_id] = timezone_component.to_tz(self, lookup_tzid=False)

    def fix_rrule_until(self, rrule: rrule, ical_rrule: prop.vRecur) -> None:
        """Make sure the until value works."""
        self.__provider.fix_rrule_until(rrule, ical_rrule)

    def create_timezone(self, timezone_component: Timezone.Timezone) -> datetime.tzinfo:
        """Create a timezone from a timezone component.

        This component will not be cached.
        """
        return self.__provider.create_timezone(timezone_component)

    def clean_timezone_id(self, tzid: str) -> str:
        """Return a clean version of the timezone id.

        Timezone ids can be a bit unclean, starting with a / for example.
        Internally, we should use this to identify timezones.
        """
        return tzid.strip("/")

    def timezone(self, tz_id: str) -> datetime.tzinfo | None:
        """Return a timezone with an id or None if we cannot find it."""
        _unclean_id = tz_id
        tz_id = self.clean_timezone_id(tz_id)
        tz = self.__provider.timezone(tz_id)
        if tz is not None:
            return tz
        if tz_id in WINDOWS_TO_OLSON:
            tz = self.__provider.timezone(WINDOWS_TO_OLSON[tz_id])
        return tz or self.__provider.timezone(_unclean_id) or self.__tz_cache.get(tz_id)

    def uses_pytz(self) -> bool:
        """Whether we use pytz at all."""
        return self.__provider.uses_pytz()

    def uses_zoneinfo(self) -> bool:
        """Whether we use zoneinfo."""
        return self.__provider.uses_zoneinfo()

    @property
    def name(self) -> str:
        """The name of the timezone component used."""
        return self.__provider.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


__all__ = ["TZP"]
