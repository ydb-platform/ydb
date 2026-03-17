""":rfc:`5545` iCalendar component."""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from icalendar.attr import (
    CONCEPTS_TYPE_SETTER,
    LINKS_TYPE_SETTER,
    RELATED_TO_TYPE_SETTER,
    categories_property,
    images_property,
    multi_language_text_property,
    single_string_property,
    source_property,
    uid_property,
    url_property,
)
from icalendar.cal.component import Component
from icalendar.cal.examples import get_example
from icalendar.cal.timezone import Timezone
from icalendar.error import IncompleteComponent
from icalendar.version import __version__

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from datetime import date, datetime

    from icalendar.cal.availability import Availability
    from icalendar.cal.event import Event
    from icalendar.cal.free_busy import FreeBusy
    from icalendar.cal.journal import Journal
    from icalendar.cal.todo import Todo


class Calendar(Component):
    """
        The "VCALENDAR" object is a collection of calendar information.
        This information can include a variety of components, such as
        "VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY", "VTIMEZONE", or any
        other type of calendar component.

    Examples:
        Create a new Calendar:

            >>> from icalendar import Calendar
            >>> calendar = Calendar.new(name="My Calendar")
            >>> print(calendar.calendar_name)
            My Calendar

    """

    name = "VCALENDAR"
    canonical_order = (
        "VERSION",
        "PRODID",
        "CALSCALE",
        "METHOD",
        "DESCRIPTION",
        "X-WR-CALDESC",
        "NAME",
        "X-WR-CALNAME",
    )
    required = (
        "PRODID",
        "VERSION",
    )
    singletons = (
        "PRODID",
        "VERSION",
        "CALSCALE",
        "METHOD",
        "COLOR",  # RFC 7986
    )
    multiple = (
        "CATEGORIES",  # RFC 7986
        "DESCRIPTION",  # RFC 7986
        "NAME",  # RFC 7986
    )

    @classmethod
    def example(cls, name: str = "example") -> Calendar:
        """Return the calendar example with the given name."""
        return cls.from_ical(get_example("calendars", name))

    @overload
    @classmethod
    def from_ical(
        cls, st: bytes | str | Path, multiple: Literal[False] = False
    ) -> Calendar: ...

    @overload
    @classmethod
    def from_ical(
        cls, st: bytes | str | Path, multiple: Literal[True]
    ) -> list[Calendar]: ...

    @classmethod
    def from_ical(
        cls,
        st: bytes | str | Path,
        multiple: bool = False,
    ) -> Calendar | list[Calendar]:
        """Parse iCalendar data into Calendar instances.

        Wraps :meth:`Component.from_ical()
        <icalendar.cal.component.Component.from_ical>` with timezone
        forward-reference resolution and VTIMEZONE caching.

        Parameters:
            st: iCalendar data as bytes or string, or a path to an iCalendar file as
                :class:`pathlib.Path` or string.
            multiple: If ``True``, returns list. If ``False``, returns single calendar.

        Returns:
            Calendar or list of Calendars
        """
        if isinstance(st, Path):
            st = st.read_bytes()
        elif isinstance(st, str) and "\n" not in st and "\r" not in st:
            path = Path(st)
            try:
                is_file = path.is_file()
            except OSError:
                is_file = False
            if is_file:
                st = path.read_bytes()

        comps = Component.from_ical(st, multiple=True)
        all_timezones_so_far = True
        for comp in comps:
            for component in comp.subcomponents:
                if component.name == "VTIMEZONE":
                    if not all_timezones_so_far:
                        # If a preceding component refers to a VTIMEZONE defined
                        # later in the source st
                        # (forward references are allowed by RFC 5545), then the
                        # earlier component may have
                        # the wrong timezone attached.
                        # However, during computation of comps, all VTIMEZONEs
                        # observed do end up in
                        # the timezone cache. So simply re-running from_ical will
                        # rely on the cache
                        # for those forward references to produce the correct result.
                        # See test_create_america_new_york_forward_reference.
                        return Component.from_ical(st, multiple)
                else:
                    all_timezones_so_far = False

        # No potentially forward VTIMEZONEs to worry about
        if multiple:
            return comps
        if len(comps) > 1:
            raise ValueError(
                cls._format_error(
                    "Found multiple components where only one is allowed", st
                )
            )
        if len(comps) < 1:
            raise ValueError(
                cls._format_error(
                    "Found no components where exactly one is required", st
                )
            )
        return comps[0]

    @property
    def events(self) -> list[Event]:
        """All event components in the calendar.

        This is a shortcut to get all events.
        Modifications do not change the calendar.
        Use :py:meth:`Component.add_component`.

        >>> from icalendar import Calendar
        >>> calendar = Calendar.example()
        >>> event = calendar.events[0]
        >>> event.start
        datetime.date(2022, 1, 1)
        >>> print(event["SUMMARY"])
        New Year's Day
        """
        return self.walk("VEVENT")

    @property
    def todos(self) -> list[Todo]:
        """All todo components in the calendar.

        This is a shortcut to get all todos.
        Modifications do not change the calendar.
        Use :py:meth:`Component.add_component`.
        """
        return self.walk("VTODO")

    @property
    def journals(self) -> list[Journal]:
        """All journal components in the calendar.

        This is a shortcut to get all journals.
        Modifications do not change the calendar.
        Use :py:meth:`Component.add_component`.
        """
        return self.walk("VJOURNAL")

    @property
    def availabilities(self) -> list[Availability]:
        """All :class:`Availability` components in the calendar.

        This is a shortcut to get all availabilities.
        Modifications do not change the calendar.
        Use :py:meth:`Component.add_component`.
        """
        return self.walk("VAVAILABILITY")

    @property
    def freebusy(self) -> list[FreeBusy]:
        """All FreeBusy components in the calendar.

        This is a shortcut to get all FreeBusy.
        Modifications do not change the calendar.
        Use :py:meth:`Component.add_component`.
        """
        return self.walk("VFREEBUSY")

    def get_used_tzids(self) -> set[str]:
        """The set of TZIDs in use.

        This goes through the whole calendar to find all occurrences of
        timezone information like the TZID parameter in all attributes.

        >>> from icalendar import Calendar
        >>> calendar = Calendar.example("timezone_rdate")
        >>> calendar.get_used_tzids()
        {'posix/Europe/Vaduz'}

        Even if you use UTC, this will not show up.
        """
        result = set()
        for _name, value in self.property_items(sorted=False):
            if hasattr(value, "params"):
                result.add(value.params.get("TZID"))
        return result - {None}

    def get_missing_tzids(self) -> set[str]:
        """The set of missing timezone component tzids.

        To create a :rfc:`5545` compatible calendar,
        all of these timezones should be added.
        """
        tzids = self.get_used_tzids()
        for timezone in self.timezones:
            tzids.remove(timezone.tz_name)
        return tzids

    @property
    def timezones(self) -> list[Timezone]:
        """Return the timezones components in this calendar.

        >>> from icalendar import Calendar
        >>> calendar = Calendar.example("pacific_fiji")
        >>> [timezone.tz_name for timezone in calendar.timezones]
        ['custom_Pacific/Fiji']

        .. note::

            This is a read-only property.
        """
        return self.walk("VTIMEZONE")

    def add_missing_timezones(
        self,
        first_date: date = Timezone.DEFAULT_FIRST_DATE,
        last_date: date = Timezone.DEFAULT_LAST_DATE,
    ):
        """Add all missing VTIMEZONE components.

        This adds all the timezone components that are required.
        VTIMEZONE components are inserted at the beginning of the calendar
        to ensure they appear before other components that reference them.

        .. note::

            Timezones that are not known will not be added.

        :param first_date: earlier than anything that happens in the calendar
        :param last_date: later than anything happening in the calendar

        >>> from icalendar import Calendar, Event
        >>> from datetime import datetime
        >>> from zoneinfo import ZoneInfo
        >>> calendar = Calendar()
        >>> event = Event()
        >>> calendar.add_component(event)
        >>> event.start = datetime(1990, 10, 11, 12, tzinfo=ZoneInfo("Europe/Berlin"))
        >>> calendar.timezones
        []
        >>> calendar.add_missing_timezones()
        >>> calendar.timezones[0].tz_name
        'Europe/Berlin'
        >>> calendar.get_missing_tzids()  # check that all are added
        set()
        """
        missing_tzids = self.get_missing_tzids()
        if not missing_tzids:
            return

        existing_timezone_count = len(self.timezones)

        for tzid in missing_tzids:
            try:
                timezone = Timezone.from_tzid(
                    tzid, first_date=first_date, last_date=last_date
                )
            except ValueError:
                continue
            self.subcomponents.insert(existing_timezone_count, timezone)
            existing_timezone_count += 1

    calendar_name = multi_language_text_property(
        "NAME",
        "X-WR-CALNAME",
        """This property specifies the name of the calendar.

    This implements :rfc:`7986` ``NAME`` and ``X-WR-CALNAME``.

    Property Parameters:
        IANA, non-standard, alternate text
        representation, and language property parameters can be specified
        on this property.

    Conformance:
        This property can be specified multiple times in an
        iCalendar object.  However, each property MUST represent the name
        of the calendar in a different language.

    Description:
        This property is used to specify a name of the
        iCalendar object that can be used by calendar user agents when
        presenting the calendar data to a user.  Whilst a calendar only
        has a single name, multiple language variants can be specified by
        including this property multiple times with different "LANGUAGE"
        parameter values on each.

    Example:
        Below, we set the name of the calendar.

        .. code-block:: pycon

            >>> from icalendar import Calendar
            >>> calendar = Calendar()
            >>> calendar.calendar_name = "My Calendar"
            >>> print(calendar.to_ical())
            BEGIN:VCALENDAR
            NAME:My Calendar
            X-WR-CALNAME:My Calendar
            END:VCALENDAR
    """,
    )

    description = multi_language_text_property(
        "DESCRIPTION",
        "X-WR-CALDESC",
        """This property specifies the description of the calendar.

    This implements :rfc:`7986` ``DESCRIPTION`` and ``X-WR-CALDESC``.

    Conformance:
        This property can be specified multiple times in an
        iCalendar object.  However, each property MUST represent the
        description of the calendar in a different language.

    Description:
        This property is used to specify a lengthy textual
        description of the iCalendar object that can be used by calendar
        user agents when describing the nature of the calendar data to a
        user.  Whilst a calendar only has a single description, multiple
        language variants can be specified by including this property
        multiple times with different "LANGUAGE" parameter values on each.

    Example:
        Below, we add a description to a calendar.

        .. code-block:: pycon

            >>> from icalendar import Calendar
            >>> calendar = Calendar()
            >>> calendar.description = "This is a calendar"
            >>> print(calendar.to_ical())
            BEGIN:VCALENDAR
            DESCRIPTION:This is a calendar
            X-WR-CALDESC:This is a calendar
            END:VCALENDAR
    """,
    )

    color = single_string_property(
        "COLOR",
        """This property specifies a color used for displaying the calendar.

    This implements :rfc:`7986` ``COLOR`` and ``X-APPLE-CALENDAR-COLOR``.
    Please note that since :rfc:`7986`, subcomponents can have their own color.

    Property Parameters:
        IANA and non-standard property parameters can
        be specified on this property.

    Conformance:
        This property can be specified once in an iCalendar
        object or in ``VEVENT``, ``VTODO``, or ``VJOURNAL`` calendar components.

    Description:
        This property specifies a color that clients MAY use
        when presenting the relevant data to a user.  Typically, this
        would appear as the "background" color of events or tasks.  The
        value is a case-insensitive color name taken from the CSS3 set of
        names, defined in Section 4.3 of `W3C.REC-css3-color-20110607 <https://www.w3.org/TR/css-color-3/>`_.

    Example:
        ``"turquoise"``, ``"#ffffff"``

        .. code-block:: pycon

            >>> from icalendar import Calendar
            >>> calendar = Calendar()
            >>> calendar.color = "black"
            >>> print(calendar.to_ical())
            BEGIN:VCALENDAR
            COLOR:black
            END:VCALENDAR

    """,
        "X-APPLE-CALENDAR-COLOR",
    )
    categories = categories_property
    uid = uid_property
    prodid = single_string_property(
        "PRODID",
        """PRODID specifies the identifier for the product that created the iCalendar object.

Conformance:
    The property MUST be specified once in an iCalendar object.

Description:
    The vendor of the implementation SHOULD assure that
    this is a globally unique identifier; using some technique such as
    an FPI value, as defined in [ISO.9070.1991].

    This property SHOULD NOT be used to alter the interpretation of an
    iCalendar object beyond the semantics specified in this memo.  For
    example, it is not to be used to further the understanding of non-
    standard properties.

Example:
    The following is an example of this property. It does not
    imply that English is the default language.

    .. code-block:: text

        -//ABC Corporation//NONSGML My Product//EN
""",
    )
    version = single_string_property(
        "VERSION",
        """VERSION of the calendar specification.

The default is ``"2.0"`` for :rfc:`5545`.

Purpose:
    This property specifies the identifier corresponding to the
    highest version number or the minimum and maximum range of the
    iCalendar specification that is required in order to interpret the
    iCalendar object.


      """,
    )

    calscale = single_string_property(
        "CALSCALE",
        """CALSCALE defines the calendar scale used for the calendar information specified in the iCalendar object.

Compatibility:
    :rfc:`7529` makes the case that GREGORIAN stays the default and other calendar scales
    are implemented on the RRULE.

Conformance:
    This property can be specified once in an iCalendar
    object.  The default value is "GREGORIAN".

Description:
    This memo is based on the Gregorian calendar scale.
    The Gregorian calendar scale is assumed if this property is not
    specified in the iCalendar object.  It is expected that other
    calendar scales will be defined in other specifications or by
    future versions of this memo.
        """,
        default="GREGORIAN",
    )
    method = single_string_property(
        "METHOD",
        """METHOD defines the iCalendar object method associated with the calendar object.

Description:
    When used in a MIME message entity, the value of this
    property MUST be the same as the Content-Type "method" parameter
    value.  If either the "METHOD" property or the Content-Type
    "method" parameter is specified, then the other MUST also be
    specified.

    No methods are defined by this specification.  This is the subject
    of other specifications, such as the iCalendar Transport-
    independent Interoperability Protocol (iTIP) defined by :rfc:`5546`.

    If this property is not present in the iCalendar object, then a
    scheduling transaction MUST NOT be assumed.  In such cases, the
    iCalendar object is merely being used to transport a snapshot of
    some calendar information; without the intention of conveying a
    scheduling semantic.
""",
    )
    url = url_property
    source = source_property

    @property
    def refresh_interval(self) -> timedelta | None:
        """REFRESH-INTERVAL specifies a suggested minimum interval for
        polling for changes of the calendar data from the original source
        of that data.

        Conformance:
            This property can be specified once in an iCalendar
            object, consisting of a positive duration of time.

        Description:
            This property specifies a positive duration that gives
            a suggested minimum polling interval for checking for updates to
            the calendar data.  The value of this property SHOULD be used by
            calendar user agents to limit the polling interval for calendar
            data updates to the minimum interval specified.

        Raises:
            ValueError: When setting a negative duration.
        """
        refresh_interval = self.get("REFRESH-INTERVAL")
        return refresh_interval.dt if refresh_interval else None

    @refresh_interval.setter
    def refresh_interval(self, value: timedelta | None):
        """Set the REFRESH-INTERVAL."""
        if not isinstance(value, timedelta) and value is not None:
            raise TypeError(
                "REFRESH-INTERVAL must be either a positive timedelta,"
                " or None to delete it."
            )
        if value is not None and value.total_seconds() <= 0:
            raise ValueError("REFRESH-INTERVAL must be a positive timedelta.")
        if value is not None:
            del self.refresh_interval
            self.add("REFRESH-INTERVAL", value)
        else:
            del self.refresh_interval

    @refresh_interval.deleter
    def refresh_interval(self):
        """Delete REFRESH-INTERVAL."""
        self.pop("REFRESH-INTERVAL")

    images = images_property

    @classmethod
    def new(
        cls,
        /,
        calscale: str | None = None,
        categories: Sequence[str] = (),
        color: str | None = None,
        concepts: CONCEPTS_TYPE_SETTER = None,
        description: str | None = None,
        language: str | None = None,
        last_modified: date | datetime | None = None,
        links: LINKS_TYPE_SETTER = None,
        method: str | None = None,
        name: str | None = None,
        organization: str | None = None,
        prodid: str | None = None,
        refresh_interval: timedelta | None = None,
        refids: list[str] | str | None = None,
        related_to: RELATED_TO_TYPE_SETTER = None,
        source: str | None = None,
        subcomponents: Iterable[Component] | None = None,
        uid: str | uuid.UUID | None = None,
        url: str | None = None,
        version: str = "2.0",
    ):
        """Create a new Calendar with all required properties.

        This creates a new Calendar in accordance with :rfc:`5545` and :rfc:`7986`.

        Parameters:
            calscale: The :attr:`calscale` of the calendar.
            categories: The :attr:`categories` of the calendar.
            color: The :attr:`color` of the calendar.
            concepts: The :attr:`~icalendar.Component.concepts` of the calendar.
            description: The :attr:`description` of the calendar.
            language: The language for the calendar. Used to generate localized `prodid`.
            last_modified: The :attr:`~icalendar.Component.last_modified` of the calendar.
            links: The :attr:`~icalendar.Component.links` of the calendar.
            method: The :attr:`method` of the calendar.
            name: The :attr:`calendar_name` of the calendar.
            organization: The organization name. Used to generate `prodid` if not provided.
            prodid: The :attr:`prodid` of the component. If None and organization is provided,
                generates a `prodid` in format "-//organization//name//language".
            refresh_interval: The :attr:`refresh_interval` of the calendar.
            refids: :attr:`~icalendar.Component.refids` of the calendar.
            related_to: :attr:`~icalendar.Component.related_to` of the calendar.
            source: The :attr:`source` of the calendar.
            subcomponents: The subcomponents of the calendar.
            uid: The :attr:`uid` of the calendar.
                If None, this is set to a new :func:`uuid.uuid4`.
            url: The :attr:`url` of the calendar.
            version: The :attr:`version` of the calendar.

        Returns:
            :class:`Calendar`

        Raises:
            ~error.InvalidCalendar: If the content is not valid according to :rfc:`5545`.

        .. warning:: As time progresses, we will be stricter with the validation.
        """
        calendar: Calendar = super().new(
            last_modified=last_modified,
            links=links,
            related_to=related_to,
            refids=refids,
            concepts=concepts,
        )

        # Generate prodid if not provided but organization is given
        if prodid is None and organization:
            app_name = name or "Calendar"
            lang = language.upper() if language else "EN"
            prodid = f"-//{organization}//{app_name}//{lang}"
        elif prodid is None:
            prodid = f"-//collective//icalendar//{__version__}//EN"

        calendar.prodid = prodid
        calendar.version = version
        calendar.calendar_name = name
        calendar.color = color
        calendar.description = description
        calendar.method = method
        calendar.calscale = calscale
        calendar.categories = categories
        calendar.uid = uid if uid is not None else uuid.uuid4()
        calendar.url = url
        calendar.refresh_interval = refresh_interval
        calendar.source = source
        if subcomponents is not None:
            calendar.subcomponents = list(subcomponents)

        return calendar

    def validate(self):
        """Validate that the calendar has required properties and components.

        This method can be called explicitly to validate a calendar before output.

        Raises:
            ~error.IncompleteComponent: If the calendar lacks required properties or
                components.
        """
        if not self.get("PRODID"):
            raise IncompleteComponent("Calendar must have a PRODID")
        if not self.get("VERSION"):
            raise IncompleteComponent("Calendar must have a VERSION")
        if not self.subcomponents:
            raise IncompleteComponent(
                "Calendar must contain at least one component (event, todo, etc.)"
            )


__all__ = ["Calendar"]
