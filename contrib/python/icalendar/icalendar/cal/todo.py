""":rfc:`5545` VTODO component."""

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
    uid_property,
    url_property,
)
from icalendar.cal.component import Component
from icalendar.cal.examples import get_example

if TYPE_CHECKING:
    from collections.abc import Sequence

    from icalendar.alarms import Alarms
    from icalendar.enums import CLASS, STATUS
    from icalendar.prop import vCalAddress
    from icalendar.prop.conference import Conference


class Todo(Component):
    """
    A "VTODO" calendar component is a grouping of component
    properties that represents an action item or assignment. For
    example, it can be used to represent an item of work assigned to
    an individual, such as "Prepare for the upcoming conference
    seminar on Internet Calendaring".

    Examples:
        Create a new Todo:

            >>> from icalendar import Todo
            >>> todo = Todo.new()
            >>> print(todo.to_ical())
            BEGIN:VTODO
            DTSTAMP:20250517T080612Z
            UID:d755cef5-2311-46ed-a0e1-6733c9e15c63
            END:VTODO

        Complete the example Todo.

        .. code-block:: pycon

            >>> from datetime import datetime, timezone
            >>> from icalendar import Todo, STATUS
            >>> todo = Todo.example()
            >>> todo["PERCENT-COMPLETE"] = 100
            >>> todo["COMPLETED"] = datetime(2007, 5, 1, 12, tzinfo=timezone.utc)
            >>> todo.status = STATUS.COMPLETED
            >>> print(todo.to_ical().decode())
            BEGIN:VTODO
            CATEGORIES:FAMILY,FINANCE
            CLASS:CONFIDENTIAL
            COMPLETED:2007-05-01 12:00:00+00:00
            DTSTAMP:20070313T123432Z
            DUE;VALUE=DATE:20070501
            PERCENT-COMPLETE:100
            STATUS:COMPLETED
            SUMMARY:Submit Quebec Income Tax Return for 2006
            UID:20070313T123432Z-456553@example.com
            END:VTODO

    """

    name = "VTODO"

    required = (
        "UID",
        "DTSTAMP",
    )
    singletons = (
        "CLASS",
        "COLOR",
        "COMPLETED",
        "CREATED",
        "DESCRIPTION",
        "DTSTAMP",
        "DTSTART",
        "GEO",
        "LAST-MODIFIED",
        "LOCATION",
        "ORGANIZER",
        "PERCENT-COMPLETE",
        "PRIORITY",
        "RECURRENCE-ID",
        "SEQUENCE",
        "STATUS",
        "SUMMARY",
        "UID",
        "URL",
        "DUE",
        "DURATION",
    )
    exclusive = (
        "DUE",
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
    DTSTART = create_single_property(
        "DTSTART",
        "dt",
        (datetime, date),
        date,
        'The "DTSTART" property for a "VTODO" specifies the inclusive start of the Todo.',
    )
    DUE = create_single_property(
        "DUE",
        "dt",
        (datetime, date),
        date,
        'The "DUE" property for a "VTODO" calendar component specifies the non-inclusive end of the Todo.',
    )
    DURATION = property(
        property_get_duration,
        property_set_duration,
        property_del_duration,
        property_doc_duration_template.format(component="VTODO"),
    )

    def _get_start_end_duration(self):
        """Verify the calendar validity and return the right attributes."""
        return get_start_end_duration_with_validation(self, "DTSTART", "DUE", "VTODO")

    @property
    def start(self) -> date | datetime:
        """The start of the VTODO.

        Invalid values raise an InvalidCalendar.
        If there is no start, we also raise an IncompleteComponent error.

        You can get the start, end and duration of a Todo as follows:

        >>> from datetime import datetime
        >>> from icalendar import Todo
        >>> todo = Todo()
        >>> todo.start = datetime(2021, 1, 1, 12)
        >>> todo.end = datetime(2021, 1, 1, 12, 30) # 30 minutes
        >>> todo.duration  # 1800 seconds == 30 minutes
        datetime.timedelta(seconds=1800)
        >>> print(todo.to_ical())
        BEGIN:VTODO
        DTSTART:20210101T120000
        DUE:20210101T123000
        END:VTODO
        """
        return get_start_property(self)

    @start.setter
    def start(self, start: date | datetime | None):
        """Set the start."""
        self.DTSTART = start

    @property
    def end(self) -> date | datetime:
        """The end of the todo.

        Invalid values raise an InvalidCalendar error.
        If there is no end, we also raise an IncompleteComponent error.
        """
        return get_end_property(self, "DUE")

    @end.setter
    def end(self, end: date | datetime | None):
        """Set the end."""
        self.DUE = end

    @property
    def duration(self) -> timedelta:
        """The duration of the VTODO.

        Returns the DURATION property if set, otherwise calculated from start and end.
        You can set the duration to automatically adjust the end time while keeping
        start locked.

        Setting the duration will:

        1.  Keep the start time locked (unchanged)
        2.  Adjust the end time to start + duration
        3.  Remove any existing DUE property
        4.  Set the DURATION property
        """
        return get_duration_property(self)

    @duration.setter
    def duration(self, value: timedelta):
        if not isinstance(value, timedelta):
            raise TypeError(f"Use timedelta, not {type(value).__name__}.")

        # Use the set_duration method with default start-locked behavior
        self.set_duration(value, locked="start")

    def set_duration(
        self, duration: timedelta | None, locked: Literal["start", "end"] = "start"
    ):
        """Set the duration of the event relative to either start or end.

        Parameters:
            duration: The duration to set, or None to convert to DURATION property
            locked: Which property to keep unchanged ('start' or 'end')
        """
        set_duration_with_locking(self, duration, locked, "DUE")

    def set_start(
        self, start: date | datetime, locked: Literal["duration", "end"] | None = None
    ):
        """Set the start with explicit locking behavior.

        Parameters:
            start: The start time to set
            locked: Which property to keep unchanged ('duration', 'end', or None
                for auto-detect)
        """
        set_start_with_locking(self, start, locked, "DUE")

    def set_end(
        self, end: date | datetime, locked: Literal["start", "duration"] = "start"
    ):
        """Set the end of the component, keeping either the start or the duration same.

        Parameters:
            end: The end time to set
            locked: Which property to keep unchanged ('start' or 'duration')
        """
        set_end_with_locking(self, end, locked, "DUE")

    X_MOZ_SNOOZE_TIME = X_MOZ_SNOOZE_TIME_property
    X_MOZ_LASTACK = X_MOZ_LASTACK_property

    @property
    def alarms(self) -> Alarms:
        """Compute the alarm times for this component.

        >>> from datetime import datetime
        >>> from icalendar import Todo
        >>> todo = Todo()  # empty without alarms
        >>> todo.start = datetime(2024, 10, 26, 10, 21)
        >>> len(todo.alarms.times)
        0

        Note that this only uses DTSTART and DUE, but ignores
        RDATE, EXDATE, and RRULE properties.
        """
        from icalendar.alarms import Alarms

        return Alarms(self)

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
        contacts: list[str] | str | None = None,
        conferences: list[Conference] | None = None,
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
        summary: str | None = None,
        uid: str | uuid.UUID | None = None,
        url: str | None = None,
    ):
        """Create a new TODO with all required properties.

        This creates a new Todo in accordance with :rfc:`5545`.

        Parameters:
            attendees: The :attr:`attendees` of the todo.
            categories: The :attr:`categories` of the todo.
            classification: The :attr:`classification` of the todo.
            color: The :attr:`color` of the todo.
            comments: The :attr:`~icalendar.Component.comments` of the todo.
            concepts: The :attr:`~icalendar.Component.concepts` of the todo.
            contacts: The :attr:`contacts` of the todo.
            conferences: The :attr:`conferences` of the todo.
            created: The :attr:`~icalendar.Component.created` of the todo.
            description: The :attr:`description` of the todo.
            end: The :attr:`end` of the todo.
            last_modified: The :attr:`~icalendar.Component.last_modified` of the todo.
            links: The :attr:`~icalendar.Component.links` of the todo.
            location: The :attr:`location` of the todo.
            organizer: The :attr:`organizer` of the todo.
            refids: :attr:`~icalendar.Component.refids` of the todo.
            related_to: :attr:`~icalendar.Component.related_to` of the todo.
            sequence: The :attr:`sequence` of the todo.
            stamp: The :attr:`~icalendar.Component.DTSTAMP` of the todo.
                If None, this is set to the current time.
            start: The :attr:`start` of the todo.
            status: The :attr:`status` of the todo.
            summary: The :attr:`summary` of the todo.
            uid: The :attr:`uid` of the todo.
                If None, this is set to a new :func:`uuid.uuid4`.
            url: The :attr:`url` of the todo.

        Returns:
            :class:`Todo`

        Raises:
            ~error.InvalidCalendar: If the content is not valid
                according to :rfc:`5545`.

        .. warning:: As time progresses, we will be stricter with the validation.
        """
        todo: Todo = super().new(
            stamp=stamp if stamp is not None else cls._utc_now(),
            created=created,
            last_modified=last_modified,
            comments=comments,
            links=links,
            related_to=related_to,
            refids=refids,
            concepts=concepts,
        )
        todo.summary = summary
        todo.description = description
        todo.uid = uid if uid is not None else uuid.uuid4()
        todo.start = start
        todo.end = end
        todo.color = color
        todo.categories = categories
        todo.sequence = sequence
        todo.classification = classification
        todo.url = url
        todo.organizer = organizer
        todo.location = location
        todo.priority = priority
        todo.contacts = contacts
        todo.status = status
        todo.attendees = attendees
        todo.conferences = conferences

        if cls._validate_new:
            cls._validate_start_and_end(start, end)
        return todo

    @classmethod
    def example(cls, name: str = "example") -> Todo:
        """Return the todo example with the given name."""
        return cls.from_ical(get_example("todos", name))


__all__ = ["Todo"]
