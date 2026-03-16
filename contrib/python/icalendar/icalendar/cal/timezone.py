""":rfc:`5545` components for timezone information."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, tzinfo
from typing import TYPE_CHECKING

import dateutil.rrule
import dateutil.tz

from icalendar.attr import (
    create_single_property,
    exdates_property,
    rdates_property,
    rrules_property,
)
from icalendar.cal.component import Component
from icalendar.cal.examples import get_example
from icalendar.prop import vUTCOffset
from icalendar.timezone import TZP, tzp
from icalendar.timezone.tzid import tzid_from_tzinfo
from icalendar.tools import is_date, to_datetime

if TYPE_CHECKING:
    from icalendar.cal.calendar import Calendar


class Timezone(Component):
    """
    A "VTIMEZONE" calendar component is a grouping of component
    properties that defines a time zone. It is used to describe the
    way in which a time zone changes its offset from UTC over time.
    """

    subcomponents: list[TimezoneStandard | TimezoneDaylight]

    name = "VTIMEZONE"
    canonical_order = ("TZID",)
    required = ("TZID",)  # it also requires one of components DAYLIGHT and STANDARD
    singletons = (
        "TZID",
        "LAST-MODIFIED",
        "TZURL",
    )

    DEFAULT_FIRST_DATE = date(1970, 1, 1)
    DEFAULT_LAST_DATE = date(2038, 1, 1)

    @classmethod
    def example(cls, name: str = "pacific_fiji") -> Calendar:
        """Return the timezone example with the given name."""
        return cls.from_ical(get_example("timezones", name))

    @staticmethod
    def _extract_offsets(component: TimezoneDaylight | TimezoneStandard, tzname: str):
        """extract offsets and transition times from a VTIMEZONE component
        :param component: a STANDARD or DAYLIGHT component
        :param tzname: the name of the zone
        """
        offsetfrom = component.TZOFFSETFROM
        offsetto = component.TZOFFSETTO
        dtstart = component.DTSTART

        # offsets need to be rounded to the next minute, we might loose up
        # to 30 seconds accuracy, but it can't be helped (datetime
        # supposedly cannot handle smaller offsets)
        offsetto_s = int((offsetto.seconds + 30) / 60) * 60
        offsetto = timedelta(days=offsetto.days, seconds=offsetto_s)
        offsetfrom_s = int((offsetfrom.seconds + 30) / 60) * 60
        offsetfrom = timedelta(days=offsetfrom.days, seconds=offsetfrom_s)

        # expand recurrences
        if "RRULE" in component:
            # to be paranoid about correct weekdays
            # evaluate the rrule with the current offset
            tzi = dateutil.tz.tzoffset("(offsetfrom)", offsetfrom)
            rrstart = dtstart.replace(tzinfo=tzi)

            rrulestr = component["RRULE"].to_ical().decode("utf-8")
            rrule = dateutil.rrule.rrulestr(rrulestr, dtstart=rrstart)
            tzp.fix_rrule_until(rrule, component["RRULE"])

            # constructing the timezone requires UTC transition times.
            # here we construct local times without tzinfo, the offset to UTC
            # gets subtracted in to_tz().
            transtimes = [dt.replace(tzinfo=None) for dt in rrule]

        # or rdates
        elif "RDATE" in component:
            if not isinstance(component["RDATE"], list):
                rdates = [component["RDATE"]]
            else:
                rdates = component["RDATE"]
            transtimes = [dtstart] + [leaf.dt for tree in rdates for leaf in tree.dts]
        else:
            transtimes = [dtstart]

        transitions = [
            (transtime, offsetfrom, offsetto, tzname) for transtime in set(transtimes)
        ]

        if component.name == "STANDARD":
            is_dst = 0
        elif component.name == "DAYLIGHT":
            is_dst = 1
        return is_dst, transitions

    @staticmethod
    def _make_unique_tzname(tzname, tznames):
        """
        :param tzname: Candidate tzname
        :param tznames: Other tznames
        """
        # TODO better way of making sure tznames are unique
        while tzname in tznames:
            tzname += "_1"
        tznames.add(tzname)
        return tzname

    def to_tz(self, tzp: TZP = tzp, lookup_tzid: bool = True):
        """convert this VTIMEZONE component to a timezone object

        :param tzp: timezone provider to use
        :param lookup_tzid: whether to use the TZID property to look up existing
                            timezone definitions with tzp.
                            If it is False, a new timezone will be created.
                            If it is True, the existing timezone will be used
                            if it exists, otherwise a new timezone will be created.
        """
        if lookup_tzid:
            tz = tzp.timezone(self.tz_name)
            if tz is not None:
                return tz
        return tzp.create_timezone(self)

    @property
    def tz_name(self) -> str:
        """Return the name of the timezone component.

        Please note that the names of the timezone are different from this name
        and may change with winter/summer time.
        """
        try:
            return str(self["TZID"])
        except UnicodeEncodeError:
            return self["TZID"].encode("ascii", "replace")

    def get_transitions(
        self,
    ) -> tuple[list[datetime], list[tuple[timedelta, timedelta, str]]]:
        """Return a tuple of (transition_times, transition_info)

        - transition_times = [datetime, ...]
        - transition_info = [(TZOFFSETTO, dts_offset, tzname)]

        """
        zone = self.tz_name
        transitions = []
        dst = {}
        tznames = set()
        for component in self.walk():
            if isinstance(component, Timezone):
                continue
            if is_date(component["DTSTART"].dt):
                component.DTSTART = to_datetime(component["DTSTART"].dt)
            assert isinstance(component["DTSTART"].dt, datetime), (
                "VTIMEZONEs sub-components' DTSTART must be of type datetime, not date"
            )
            try:
                tzname = str(component["TZNAME"])
            except UnicodeEncodeError:
                tzname = component["TZNAME"].encode("ascii", "replace")
                tzname = self._make_unique_tzname(tzname, tznames)
            except KeyError:
                # for whatever reason this is str/unicode
                tzname = (
                    f"{zone}_{component['DTSTART'].to_ical().decode('utf-8')}_"
                    f"{component['TZOFFSETFROM'].to_ical()}_"
                    f"{component['TZOFFSETTO'].to_ical()}"
                )
                tzname = self._make_unique_tzname(tzname, tznames)

            dst[tzname], component_transitions = self._extract_offsets(
                component, tzname
            )
            transitions.extend(component_transitions)

        transitions.sort()
        transition_times = [
            transtime - osfrom for transtime, osfrom, _, _ in transitions
        ]

        # transition_info is a list with tuples in the format
        # (utcoffset, dstoffset, name)
        # dstoffset = 0, if current transition is to standard time
        #           = this_utcoffset - prev_standard_utcoffset, otherwise
        transition_info = []
        for num, (_transtime, osfrom, osto, name) in enumerate(transitions):
            dst_offset = False
            if not dst[name]:
                dst_offset = timedelta(seconds=0)
            else:
                # go back in time until we find a transition to dst
                for index in range(num - 1, -1, -1):
                    if not dst[transitions[index][3]]:  # [3] is the name
                        dst_offset = osto - transitions[index][2]  # [2] is osto
                        break
                # when the first transition is to dst, we didn't find anything
                # in the past, so we have to look into the future
                if not dst_offset:
                    for index in range(num, len(transitions)):
                        if not dst[transitions[index][3]]:  # [3] is the name
                            dst_offset = osto - transitions[index][2]  # [2] is osto
                            break
                # If we still haven't found a STANDARD transition
                # (only DAYLIGHT exists), calculate dst_offset as the
                # difference from TZOFFSETFROM. Handles Issue #321.
                if dst_offset is False:
                    dst_offset = osto - osfrom
            transition_info.append((osto, dst_offset, name))
        return transition_times, transition_info

    # binary search
    _from_tzinfo_skip_search = [
        timedelta(days=days) for days in (64, 32, 16, 8, 4, 2, 1)
    ] + [
        # we know it happens in the night usually around 1am
        timedelta(hours=4),
        timedelta(hours=1),
        # adding some minutes and seconds for faster search
        timedelta(minutes=20),
        timedelta(minutes=5),
        timedelta(minutes=1),
        timedelta(seconds=20),
        timedelta(seconds=5),
        timedelta(seconds=1),
    ]

    @classmethod
    def from_tzinfo(
        cls,
        timezone: tzinfo,
        tzid: str | None = None,
        first_date: date = DEFAULT_FIRST_DATE,
        last_date: date = DEFAULT_LAST_DATE,
    ) -> Timezone:
        """Return a VTIMEZONE component from a timezone object.

        This works with pytz and zoneinfo and any other timezone.
        The offsets are calculated from the tzinfo object.

        Parameters:

        :param tzinfo: the timezone object
        :param tzid: the tzid for this timezone. If None, it will be extracted from the tzinfo.
        :param first_date: a datetime that is earlier than anything that happens in the calendar
        :param last_date: a datetime that is later than anything that happens in the calendar
        :raises ValueError: If we have no tzid and cannot extract one.

        .. note::
            This can take some time. Please cache the results.
        """
        if tzid is None:
            tzid = tzid_from_tzinfo(timezone)
            if tzid is None:
                raise ValueError(
                    f"Cannot get TZID from {timezone}. Please set the tzid parameter."
                )
        normalize = getattr(timezone, "normalize", lambda dt: dt)  # pytz compatibility
        first_datetime = datetime(first_date.year, first_date.month, first_date.day)
        last_datetime = datetime(last_date.year, last_date.month, last_date.day)
        if hasattr(timezone, "localize"):  # pytz compatibility
            first_datetime = timezone.localize(first_datetime)
            last_datetime = timezone.localize(last_datetime)
        else:
            first_datetime = first_datetime.replace(tzinfo=timezone)
            last_datetime = last_datetime.replace(tzinfo=timezone)
        # from, to, tzname, is_standard -> start
        offsets: dict[tuple[timedelta | None, timedelta, str, bool], list[datetime]] = (
            defaultdict(list)
        )
        start = first_datetime
        offset_to = None
        while start < last_datetime:
            offset_from = offset_to
            end = start
            offset_to = end.utcoffset()
            for add_offset in cls._from_tzinfo_skip_search:
                last_end = end  # we need to save this as we might be left and right of the time change
                end = normalize(end + add_offset)
                try:
                    while end.utcoffset() == offset_to:
                        last_end = end
                        end = normalize(end + add_offset)
                except OverflowError:
                    # zoninfo does not go all the way
                    break
                # retract if we overshoot
                end = last_end
            # Now, start (inclusive) -> end (exclusive) are one timezone
            is_standard = start.dst() == timedelta()
            name = start.tzname()
            if name is None:
                name = str(offset_to)
            key = (offset_from, offset_to, name, is_standard)
            # first_key = (None,) + key[1:]
            # if first_key in offsets:
            #     # remove the first one and claim it changes at that day
            #     offsets[first_key] = offsets.pop(first_key)
            offsets[key].append(start.replace(tzinfo=None))
            start = normalize(end + cls._from_tzinfo_skip_search[-1])
        tz = cls()
        tz.add("TZID", tzid)
        tz.add("COMMENT", f"This timezone only works from {first_date} to {last_date}.")
        for (offset_from, offset_to, tzname, is_standard), starts in offsets.items():
            first_start = min(starts)
            starts.remove(first_start)
            if first_start.date() == last_date:
                first_start = datetime(last_date.year, last_date.month, last_date.day)
            subcomponent = TimezoneStandard() if is_standard else TimezoneDaylight()
            if offset_from is None:
                offset_from = offset_to
            subcomponent.TZOFFSETFROM = offset_from
            subcomponent.TZOFFSETTO = offset_to
            subcomponent.add("TZNAME", tzname)
            subcomponent.DTSTART = first_start
            if starts:
                subcomponent.add("RDATE", starts)
            tz.add_component(subcomponent)
        return tz

    @classmethod
    def from_tzid(
        cls,
        tzid: str,
        tzp: TZP = tzp,
        first_date: date = DEFAULT_FIRST_DATE,
        last_date: date = DEFAULT_LAST_DATE,
    ) -> Timezone:
        """Create a VTIMEZONE from a tzid like ``"Europe/Berlin"``.

        :param tzid: the id of the timezone
        :param tzp: the timezone provider
        :param first_date: a datetime that is earlier than anything
            that happens in the calendar
        :param last_date: a datetime that is later than anything
            that happens in the calendar
        :raises ValueError: If the tzid is unknown.

        >>> from icalendar import Timezone
        >>> tz = Timezone.from_tzid("Europe/Berlin")
        >>> print(tz.to_ical()[:36])
        BEGIN:VTIMEZONE
        TZID:Europe/Berlin

        .. note::
            This can take some time. Please cache the results.
        """
        tz = tzp.timezone(tzid)
        if tz is None:
            raise ValueError(f"Unkown timezone {tzid}.")
        return cls.from_tzinfo(tz, tzid, first_date, last_date)

    @property
    def standard(self) -> list[TimezoneStandard]:
        """The STANDARD subcomponents as a list."""
        return self.walk("STANDARD")

    @property
    def daylight(self) -> list[TimezoneDaylight]:
        """The DAYLIGHT subcomponents as a list.

        These are for the daylight saving time.
        """
        return self.walk("DAYLIGHT")


class TimezoneStandard(Component):
    """
    The "STANDARD" sub-component of "VTIMEZONE" defines the standard
    time offset from UTC for a time zone. It represents a time zone's
    standard time, typically used during winter months in locations
    that observe Daylight Saving Time.
    """

    name = "STANDARD"
    required = ("DTSTART", "TZOFFSETTO", "TZOFFSETFROM")
    singletons = (
        "DTSTART",
        "TZOFFSETTO",
        "TZOFFSETFROM",
    )
    multiple = ("COMMENT", "RDATE", "TZNAME", "RRULE", "EXDATE")

    DTSTART = create_single_property(
        "DTSTART",
        "dt",
        (datetime,),
        datetime,
        """The mandatory "DTSTART" property gives the effective onset date
        and local time for the time zone sub-component definition.
        "DTSTART" in this usage MUST be specified as a date with a local
        time value.""",
    )
    TZOFFSETTO = create_single_property(
        "TZOFFSETTO",
        "td",
        (timedelta,),
        timedelta,
        """The mandatory "TZOFFSETTO" property gives the UTC offset for the
        time zone sub-component (Standard Time or Daylight Saving Time)
        when this observance is in use.
        """,
        vUTCOffset,
    )
    TZOFFSETFROM = create_single_property(
        "TZOFFSETFROM",
        "td",
        (timedelta,),
        timedelta,
        """The mandatory "TZOFFSETFROM" property gives the UTC offset that is
        in use when the onset of this time zone observance begins.
        "TZOFFSETFROM" is combined with "DTSTART" to define the effective
        onset for the time zone sub-component definition.  For example,
        the following represents the time at which the observance of
        Standard Time took effect in Fall 1967 for New York City:

            DTSTART:19671029T020000
            TZOFFSETFROM:-0400
        """,
        vUTCOffset,
    )
    rdates = rdates_property
    exdates = exdates_property
    rrules = rrules_property


class TimezoneDaylight(Component):
    """
    The "DAYLIGHT" sub-component of "VTIMEZONE" defines the daylight
    saving time offset from UTC for a time zone. It represents a time
    zone's daylight saving time, typically used during summer months
    in locations that observe Daylight Saving Time.
    """

    name = "DAYLIGHT"
    required = TimezoneStandard.required
    singletons = TimezoneStandard.singletons
    multiple = TimezoneStandard.multiple

    DTSTART = TimezoneStandard.DTSTART
    TZOFFSETTO = TimezoneStandard.TZOFFSETTO
    TZOFFSETFROM = TimezoneStandard.TZOFFSETFROM

    rdates = rdates_property
    exdates = exdates_property
    rrules = rrules_property


__all__ = ["Timezone", "TimezoneDaylight", "TimezoneStandard"]
