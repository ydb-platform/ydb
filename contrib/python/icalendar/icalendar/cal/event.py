""":rfc:`5545` VEVENT component."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Literal

from icalendar.attr import (
    CONCEPTS_TYPE_SETTER,
    LINKS_TYPE_SETTER,
    RELATED_TO_TYPE_SETTER,
    X_MOZ_LASTACK_property,
    X_MOZ_SNOOZE_TIME_property,
    attendees_property,
    categories_property,
    class_property,
    color_property,
    conferences_property,
    contacts_property,
    create_single_property,
    description_property,
    exdates_property,
    get_duration_property,
    get_end_property,
    get_start_end_duration_with_validation,
    get_start_property,
    images_property,
    location_property,
    organizer_property,
    priority_property,
    property_del_duration,
    property_doc_duration_template,
    property_get_duration,
    property_set_duration,
    rdates_property,
    rrules_property,
    sequence_property,
    set_duration_with_locking,
    set_end_with_locking,
    set_start_with_locking,
    status_property,
    summary_property,
    transparency_property,
    uid_property,
    url_property,
)
from icalendar.cal.component import Component
from icalendar.cal.examples import get_example

if TYPE_CHECKING:
    from collections.abc import Sequence

    from icalendar.alarms import Alarms
    from icalendar.enums import CLASS, STATUS, TRANSP
    from icalendar.prop import vCalAddress
    from icalendar.prop.conference import Conference


class Event(Component):
    """A grouping of component properties that describe an event.

    Description:
        A "VEVENT" calendar component is a grouping of
        component properties, possibly including "VALARM" calendar
        components, that represents a scheduled amount of time on a
        calendar.  For example, it can be an activity; such as a one-hour
        long, department meeting from 8:00 AM to 9:00 AM, tomorrow.
        Generally, an event will take up time on an individual calendar.
        Hence, the event will appear as an opaque interval in a search for
        busy time.  Alternately, the event can have its Time Transparency
        set to "TRANSPARENT" in order to prevent blocking of the event in
        searches for busy time.

        The "VEVENT" is also the calendar component used to specify an
        anniversary or daily reminder within a calendar.  These events
        have a DATE value type for the "DTSTART" property instead of the
        default value type of DATE-TIME.  If such a "VEVENT" has a "DTEND"
        property, it MUST be specified as a DATE value also.  The
        anniversary type of "VEVENT" can span more than one date (i.e.,
        "DTEND" property value is set to a calendar date after the
        "DTSTART" property value).  If such a "VEVENT" has a "DURATION"
        property, it MUST be specified as a "dur-day" or "dur-week" value.

        The "DTSTART" property for a "VEVENT" specifies the inclusive
        start of the event.  For recurring events, it also specifies the
        very first instance in the recurrence set.  The "DTEND" property
        for a "VEVENT" calendar component specifies the non-inclusive end
        of the event.  For cases where a "VEVENT" calendar component
        specifies a "DTSTART" property with a DATE value type but no
        "DTEND" nor "DURATION" property, the event's duration is taken to
        be one day.  For cases where a "VEVENT" calendar component
        specifies a "DTSTART" property with a DATE-TIME value type but no
        "DTEND" property, the event ends on the same calendar date and
        time of day specified by the "DTSTART" property.

        The "VEVENT" calendar component cannot be nested within another
        calendar component.  However, "VEVENT" calendar components can be
        related to each other or to a "VTODO" or to a "VJOURNAL" calendar
        component with the "RELATED-TO" property.

    Examples:
        The following is an example of the "VEVENT" calendar
        component used to represent a meeting that will also be opaque to
        searches for busy time:

        .. code-block:: text

            BEGIN:VEVENT
            UID:19970901T130000Z-123401@example.com
            DTSTAMP:19970901T130000Z
            DTSTART:19970903T163000Z
            DTEND:19970903T190000Z
            SUMMARY:Annual Employee Review
            CLASS:PRIVATE
            CATEGORIES:BUSINESS,HUMAN RESOURCES
            END:VEVENT

        The following is an example of the "VEVENT" calendar component
        used to represent a reminder that will not be opaque, but rather
        transparent, to searches for busy time:

        .. code-block:: text

            BEGIN:VEVENT
            UID:19970901T130000Z-123402@example.com
            DTSTAMP:19970901T130000Z
            DTSTART:19970401T163000Z
            DTEND:19970402T010000Z
            SUMMARY:Laurel is in sensitivity awareness class.
            CLASS:PUBLIC
            CATEGORIES:BUSINESS,HUMAN RESOURCES
            TRANSP:TRANSPARENT
            END:VEVENT

        The following is an example of the "VEVENT" calendar component
        used to represent an anniversary that will occur annually:

        .. code-block:: text

            BEGIN:VEVENT
            UID:19970901T130000Z-123403@example.com
            DTSTAMP:19970901T130000Z
            DTSTART;VALUE=DATE:19971102
            SUMMARY:Our Blissful Anniversary
            TRANSP:TRANSPARENT
            CLASS:CONFIDENTIAL
            CATEGORIES:ANNIVERSARY,PERSONAL,SPECIAL OCCASION
            RRULE:FREQ=YEARLY
            END:VEVENT

        The following is an example of the "VEVENT" calendar component
        used to represent a multi-day event scheduled from June 28th, 2007
        to July 8th, 2007 inclusively.  Note that the "DTEND" property is
        set to July 9th, 2007, since the "DTEND" property specifies the
        non-inclusive end of the event.

        .. code-block:: text

            BEGIN:VEVENT
            UID:20070423T123432Z-541111@example.com
            DTSTAMP:20070423T123432Z
            DTSTART;VALUE=DATE:20070628
            DTEND;VALUE=DATE:20070709
            SUMMARY:Festival International de Jazz de Montreal
            TRANSP:TRANSPARENT
            END:VEVENT

        Create a new Event:

        .. code-block:: python

            >>> from icalendar import Event
            >>> from datetime import datetime
            >>> event = Event.new(start=datetime(2021, 1, 1, 12, 30, 0))
            >>> print(event.to_ical())
            BEGIN:VEVENT
            DTSTART:20210101T123000
            DTSTAMP:20250517T080612Z
            UID:d755cef5-2311-46ed-a0e1-6733c9e15c63
            END:VEVENT

    """

    name = "VEVENT"

    canonical_order = (
        "SUMMARY",
        "DTSTART",
        "DTEND",
        "DURATION",
        "DTSTAMP",
        "UID",
        "RECURRENCE-ID",
        "SEQUENCE",
        "RRULE",
        "RDATE",
        "EXDATE",
    )

    required = (
        "UID",
        "DTSTAMP",
    )
    singletons = (
        "CLASS",
        "CREATED",
        "COLOR",
        "DESCRIPTION",
        "DTSTART",
        "GEO",
        "LAST-MODIFIED",
        "LOCATION",
        "ORGANIZER",
        "PRIORITY",
        "DTSTAMP",
        "SEQUENCE",
        "STATUS",
        "SUMMARY",
        "TRANSP",
        "URL",
        "RECURRENCE-ID",
        "DTEND",
        "DURATION",
        "UID",
    )
    exclusive = (
        "DTEND",
        "DURATION",
    )
    multiple = (
        "ATTACH",
        "ATTENDEE",
        "CATEGORIES",
        "COMMENT",
        "CONTACT",
        "EXDATE",
        "RSTATUS",
        "RELATED",
        "RESOURCES",
        "RDATE",
        "RRULE",
    )
    ignore_exceptions = True

    @property
    def alarms(self) -> Alarms:
        """Compute the alarm times for this component.

        >>> from icalendar import Event
        >>> event = Event.example("rfc_9074_example_1")
        >>> len(event.alarms.times)
        1
        >>> alarm_time = event.alarms.times[0]
        >>> alarm_time.trigger  # The time when the alarm pops up
        datetime.datetime(2021, 3, 2, 10, 15, tzinfo=ZoneInfo(key='America/New_York'))
        >>> alarm_time.is_active()  # This alarm has not been acknowledged
        True

        Note that this only uses DTSTART and DTEND, but ignores
        RDATE, EXDATE, and RRULE properties.
        """
        from icalendar.alarms import Alarms

        return Alarms(self)

    @classmethod
    def example(cls, name: str = "rfc_9074_example_3") -> Event:
        """Return the calendar example with the given name."""
        return cls.from_ical(get_example("events", name))

    DTSTART = create_single_property(
        "DTSTART",
        "dt",
        (datetime, date),
        date,
        'The "DTSTART" property for a "VEVENT" specifies the inclusive start of the event.',
    )
    DTEND = create_single_property(
        "DTEND",
        "dt",
        (datetime, date),
        date,
        'The "DTEND" property for a "VEVENT" calendar component specifies the non-inclusive end of the event.',
    )

    def _get_start_end_duration(self):
        """Verify the calendar validity and return the right attributes."""
        return get_start_end_duration_with_validation(
            self, "DTSTART", "DTEND", "VEVENT"
        )

    DURATION = property(
        property_get_duration,
        property_set_duration,
        property_del_duration,
        property_doc_duration_template.format(component="VEVENT"),
    )

    @property
    def duration(self) -> timedelta:
        """The duration of the VEVENT.

        Returns the DURATION property if set, otherwise calculated from start and end.
        When setting duration, the end time is automatically calculated from start +
        duration.

        You can set the duration to automatically adjust the end time while keeping
        start locked.

        Setting the duration will:

        1.  Keep the start time locked (unchanged)
        2.  Adjust the end time to start + duration
        3.  Remove any existing DTEND property
        4.  Set the DURATION property
        """
        return get_duration_property(self)

    @duration.setter
    def duration(self, value: timedelta):
        if not isinstance(value, timedelta):
            raise TypeError(f"Use timedelta, not {type(value).__name__}.")

        # Use the set_duration method with default start-locked behavior
        self.set_duration(value, locked="start")

    @property
    def start(self) -> date | datetime:
        """The start of the event.

        Invalid values raise an InvalidCalendar.
        If there is no start, we also raise an IncompleteComponent error.

        You can get the start, end and duration of an event as follows:

        >>> from datetime import datetime
        >>> from icalendar import Event
        >>> event = Event()
        >>> event.start = datetime(2021, 1, 1, 12)
        >>> event.end = datetime(2021, 1, 1, 12, 30) # 30 minutes
        >>> event.duration  # 1800 seconds == 30 minutes
        datetime.timedelta(seconds=1800)
        >>> print(event.to_ical())
        BEGIN:VEVENT
        DTSTART:20210101T120000
        DTEND:20210101T123000
        END:VEVENT
        """
        return get_start_property(self)

    @start.setter
    def start(self, start: date | datetime | None):
        """Set the start."""
        self.DTSTART = start

    @property
    def end(self) -> date | datetime:
        """The end of the event.

        Invalid values raise an InvalidCalendar error.
        If there is no end, we also raise an IncompleteComponent error.
        """
        return get_end_property(self, "DTEND")

    @end.setter
    def end(self, end: date | datetime | None):
        """Set the end."""
        self.DTEND = end

    def set_duration(
        self, duration: timedelta | None, locked: Literal["start", "end"] = "start"
    ):
        """Set the duration of the event relative to either start or end.

        Parameters:
            duration: The duration to set, or None to convert to DURATION property
            locked: Which property to keep unchanged ('start' or 'end')
        """
        set_duration_with_locking(self, duration, locked, "DTEND")

    def set_start(
        self, start: date | datetime, locked: Literal["duration", "end"] | None = None
    ):
        """Set the start and keep the duration or end of the event.

        Parameters:
            start: The start time to set
            locked: Which property to keep unchanged ('duration', 'end', or None
                for auto-detect)
        """
        set_start_with_locking(self, start, locked, "DTEND")

    def set_end(
        self, end: date | datetime, locked: Literal["start", "duration"] = "start"
    ):
        """Set the end of the component, keeping either the start or the duration same.

        Parameters:
            end: The end time to set
            locked: Which property to keep unchanged ('start' or 'duration')
        """
        set_end_with_locking(self, end, locked, "DTEND")

    X_MOZ_SNOOZE_TIME = X_MOZ_SNOOZE_TIME_property
    X_MOZ_LASTACK = X_MOZ_LASTACK_property
    color = color_property
    sequence = sequence_property
    categories = categories_property
    rdates = rdates_property
    exdates = exdates_property
    rrules = rrules_property
    uid = uid_property
    summary = summary_property
    description = description_property
    classification = class_property
    url = url_property
    organizer = organizer_property
    location = location_property
    priority = priority_property
    contacts = contacts_property
    transparency = transparency_property
    status = status_property
    attendees = attendees_property
    images = images_property
    conferences = conferences_property

    @classmethod
    def new(
        cls,
        /,
        attendees: list[vCalAddress] | None = None,
        categories: Sequence[str] = (),
        classification: CLASS | None = None,
        color: str | None = None,
        comments: list[str] | str | None = None,
        concepts: CONCEPTS_TYPE_SETTER = None,
        conferences: list[Conference] | None = None,
        contacts: list[str] | str | None = None,
        created: date | None = None,
        description: str | None = None,
        end: date | datetime | None = None,
        last_modified: date | None = None,
        links: LINKS_TYPE_SETTER = None,
        location: str | None = None,
        organizer: vCalAddress | str | None = None,
        priority: int | None = None,
        refids: list[str] | str | None = None,
        related_to: RELATED_TO_TYPE_SETTER = None,
        sequence: int | None = None,
        stamp: date | None = None,
        start: date | datetime | None = None,
        status: STATUS | None = None,
        transparency: TRANSP | None = None,
        summary: str | None = None,
        uid: str | uuid.UUID | None = None,
        url: str | None = None,
    ):
        """Create a new event with all required properties.

        This creates a new Event in accordance with :rfc:`5545`.

        Parameters:
            attendees: The :attr:`attendees` of the event.
            categories: The :attr:`categories` of the event.
            classification: The :attr:`classification` of the event.
            color: The :attr:`color` of the event.
            comments: The :attr:`~icalendar.Component.comments` of the event.
            concepts: The :attr:`~icalendar.Component.concepts` of the event.
            conferences: The :attr:`conferences` of the event.
            created: The :attr:`~icalendar.Component.created` of the event.
            description: The :attr:`description` of the event.
            end: The :attr:`end` of the event.
            last_modified: The :attr:`~icalendar.Component.last_modified` of the event.
            links: The :attr:`~icalendar.Component.links` of the event.
            location: The :attr:`location` of the event.
            organizer: The :attr:`organizer` of the event.
            priority: The :attr:`priority` of the event.
            refids: :attr:`~icalendar.Component.refids` of the event.
            related_to: :attr:`~icalendar.Component.related_to` of the event.
            sequence: The :attr:`sequence` of the event.
            stamp: The :attr:`~icalendar.Component.stamp` of the event.
                If None, this is set to the current time.
            start: The :attr:`start` of the event.
            status: The :attr:`status` of the event.
            summary: The :attr:`summary` of the event.
            transparency: The :attr:`transparency` of the event.
            uid: The :attr:`uid` of the event.
                If None, this is set to a new :func:`uuid.uuid4`.
            url: The :attr:`url` of the event.

        Returns:
            :class:`Event`

        Raises:
            ~error.InvalidCalendar: If the content is not valid
                according to :rfc:`5545`.

        .. warning:: As time progresses, we will be stricter with the validation.
        """
        event: Event = super().new(
            stamp=stamp if stamp is not None else cls._utc_now(),
            created=created,
            last_modified=last_modified,
            comments=comments,
            links=links,
            related_to=related_to,
            refids=refids,
            concepts=concepts,
        )
        event.summary = summary
        event.description = description
        event.uid = uid if uid is not None else uuid.uuid4()
        event.start = start
        event.end = end
        event.color = color
        event.categories = categories
        event.sequence = sequence
        event.classification = classification
        event.url = url
        event.organizer = organizer
        event.location = location
        event.priority = priority
        event.transparency = transparency
        event.contacts = contacts
        event.status = status
        event.attendees = attendees
        event.conferences = conferences

        if cls._validate_new:
            cls._validate_start_and_end(start, end)
        return event


__all__ = ["Event"]
