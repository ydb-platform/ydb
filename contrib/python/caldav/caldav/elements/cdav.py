#!/usr/bin/env python
import logging
from datetime import datetime
from datetime import timezone
from typing import ClassVar
from typing import Optional

from .base import BaseElement
from .base import NamedBaseElement
from .base import ValuedBaseElement
from caldav.lib.namespace import ns

utc_tz = timezone.utc


def _to_utc_date_string(ts):
    # type (Union[date,datetime]]) -> str
    """coerce datetimes to UTC (assume localtime if nothing is given)"""
    if isinstance(ts, datetime):
        try:
            ## for any python version, this should work for a non-native
            ## timestamp.
            ## in python 3.6 and higher, ts.astimezone() will assume a
            ## naive timestamp is localtime (and so do we)
            ts = ts.astimezone(utc_tz)
        except:
            ## native time stamp and the current python version is
            ## not able to treat it as localtime.
            import tzlocal

            ts = ts.replace(tzinfo=tzlocal.get_localzone())

            mindate = datetime.min.replace(tzinfo=utc_tz)
            maxdate = datetime.max.replace(tzinfo=utc_tz)
            if mindate + ts.tzinfo.utcoffset(ts) > ts:
                logging.error(
                    "Cannot coerce datetime %s to UTC. Changed to min-date.", ts
                )
                ts = mindate
            elif ts > maxdate - ts.tzinfo.utcoffset(ts):
                logging.error(
                    "Cannot coerce datetime %s to UTC. Changed to max-date.", ts
                )
                ts = maxdate
            else:
                ts = ts.astimezone(utc_tz)

    return ts.strftime("%Y%m%dT%H%M%SZ")


# Operations
class CalendarQuery(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar-query")


class FreeBusyQuery(BaseElement):
    tag: ClassVar[str] = ns("C", "free-busy-query")


class Mkcalendar(BaseElement):
    tag: ClassVar[str] = ns("C", "mkcalendar")


class CalendarMultiGet(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar-multiget")


class ScheduleInboxURL(BaseElement):
    tag: ClassVar[str] = ns("C", "schedule-inbox-URL")


class ScheduleOutboxURL(BaseElement):
    tag: ClassVar[str] = ns("C", "schedule-outbox-URL")


# Filters
class Filter(BaseElement):
    tag: ClassVar[str] = ns("C", "filter")


class CompFilter(NamedBaseElement):
    tag: ClassVar[str] = ns("C", "comp-filter")


class PropFilter(NamedBaseElement):
    tag: ClassVar[str] = ns("C", "prop-filter")


class ParamFilter(NamedBaseElement):
    tag: ClassVar[str] = ns("C", "param-filter")


# Conditions
class TextMatch(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "text-match")

    def __init__(self, value, collation: str = "i;octet", negate: bool = False) -> None:
        super(TextMatch, self).__init__(value=value)

        if self.attributes is None:
            raise ValueError("Unexpected value None for self.attributes")

        self.attributes["collation"] = collation
        if negate:
            self.attributes["negate-condition"] = "yes"


class TimeRange(BaseElement):
    tag: ClassVar[str] = ns("C", "time-range")

    def __init__(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None
    ) -> None:
        ## start and end should be an icalendar "date with UTC time",
        ## ref https://tools.ietf.org/html/rfc4791#section-9.9
        super(TimeRange, self).__init__()

        if self.attributes is None:
            raise ValueError("Unexpected value None for self.attributes")

        if start is not None:
            self.attributes["start"] = _to_utc_date_string(start)
        if end is not None:
            self.attributes["end"] = _to_utc_date_string(end)


class NotDefined(BaseElement):
    tag: ClassVar[str] = ns("C", "is-not-defined")


# Components / Data
class CalendarData(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar-data")


class Expand(BaseElement):
    tag: ClassVar[str] = ns("C", "expand")

    def __init__(
        self, start: Optional[datetime], end: Optional[datetime] = None
    ) -> None:
        super(Expand, self).__init__()

        if self.attributes is None:
            raise ValueError("Unexpected value None for self.attributes")

        if start is not None:
            self.attributes["start"] = _to_utc_date_string(start)
        if end is not None:
            self.attributes["end"] = _to_utc_date_string(end)


class Comp(NamedBaseElement):
    tag: ClassVar[str] = ns("C", "comp")


# Uhhm ... can't find any references to calendar-collection in rfc4791.txt
# and newer versions of baikal gives 403 forbidden when this one is
# encountered
# class CalendarCollection(BaseElement):
#     tag = ns("C", "calendar-collection")


# Properties
class CalendarUserAddressSet(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar-user-address-set")


class CalendarUserType(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar-user-type")


class CalendarHomeSet(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar-home-set")


# calendar resource type, see rfc4791, sec. 4.2
class Calendar(BaseElement):
    tag: ClassVar[str] = ns("C", "calendar")


class CalendarDescription(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "calendar-description")


class CalendarTimeZone(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "calendar-timezone")


class SupportedCalendarComponentSet(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "supported-calendar-component-set")


class SupportedCalendarData(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "supported-calendar-data")


class MaxResourceSize(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "max-resource-size")


class MinDateTime(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "min-date-time")


class MaxDateTime(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "max-date-time")


class MaxInstances(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "max-instances")


class MaxAttendeesPerInstance(ValuedBaseElement):
    tag: ClassVar[str] = ns("C", "max-attendees-per-instance")


class Allprop(BaseElement):
    tag: ClassVar[str] = ns("C", "allprop")


class ScheduleTag(BaseElement):
    tag: ClassVar[str] = ns("C", "schedule-tag")
