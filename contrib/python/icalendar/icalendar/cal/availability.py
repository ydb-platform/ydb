"""This implementes the VAVAILABILITY component.

This is specified in :rfc:`7953`.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from icalendar.attr import (
    CONCEPTS_TYPE_SETTER,
    LINKS_TYPE_SETTER,
    RELATED_TO_TYPE_SETTER,
    busy_type_property,
    categories_property,
    class_property,
    contacts_property,
    description_property,
    duration_property,
    location_property,
    organizer_property,
    priority_property,
    rfc_7953_dtend_property,
    rfc_7953_dtstart_property,
    rfc_7953_duration_property,
    rfc_7953_end_property,
    sequence_property,
    summary_property,
    url_property,
)
from icalendar.cal.examples import get_example
from icalendar.error import InvalidCalendar

from .component import Component

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date

    from icalendar.cal.available import Available
    from icalendar.enums import BUSYTYPE, CLASS
    from icalendar.prop import vCalAddress


class Availability(Component):
    """VAVAILABILITY component from :rfc:`7953`.

    This provides a grouping of component properties and
    subcomponents that describe the availability associated with a
    calendar user.

    Description:
        A "VAVAILABILITY" component indicates a period of time
        within which availability information is provided.  A
        "VAVAILABILITY" component can specify a start time and an end time
        or duration.  If "DTSTART" is not present, then the start time is
        unbounded.  If "DTEND" or "DURATION" are not present, then the end
        time is unbounded.  Within the specified time period, availability
        defaults to a free-busy type of "BUSY-UNAVAILABLE" (see
        Section 3.2), except for any time periods corresponding to
        "AVAILABLE" subcomponents.

        "AVAILABLE" subcomponents are used to indicate periods of free
        time within the time range of the enclosing "VAVAILABILITY"
        component.  "AVAILABLE" subcomponents MAY include recurrence
        properties to specify recurring periods of time, which can be
        overridden using normal iCalendar recurrence behavior (i.e., use
        of the "RECURRENCE-ID" property).

        If specified, the "DTSTART" and "DTEND" properties in
        "VAVAILABILITY" components and "AVAILABLE" subcomponents MUST be
        "DATE-TIME" values specified as either the date with UTC time or
        the date with local time and a time zone reference.

        The iCalendar object containing the "VAVAILABILITY" component MUST
        contain appropriate "VTIMEZONE" components corresponding to each
        unique "TZID" parameter value used in any DATE-TIME properties in
        all components, unless [RFC7809] is in effect.

        When used to publish available time, the "ORGANIZER" property
        specifies the calendar user associated with the published
        available time.

        If the "PRIORITY" property is specified in "VAVAILABILITY"
        components, it is used to determine how that component is combined
        with other "VAVAILABILITY" components.  See Section 4.

        Other calendar properties MAY be specified in "VAVAILABILITY" or
        "AVAILABLE" components and are considered attributes of the marked
        block of time.  Their usage is application specific.  For example,
        the "LOCATION" property might be used to indicate that a person is
        available in one location for part of the week and a different
        location for another part of the week (but see Section 9 for when
        it is appropriate to add additional data like this).

    Example:
        The following is an example of a "VAVAILABILITY" calendar
        component used to represent the availability of a user, always
        available Monday through Friday, 9:00 am to 5:00 pm in the
        America/Montreal time zone:

        .. code-block:: text

            BEGIN:VAVAILABILITY
            ORGANIZER:mailto:bernard@example.com
            UID:0428C7D2-688E-4D2E-AC52-CD112E2469DF
            DTSTAMP:20111005T133225Z
            BEGIN:AVAILABLE
            UID:34EDA59B-6BB1-4E94-A66C-64999089C0AF
            SUMMARY:Monday to Friday from 9:00 to 17:00
            DTSTART;TZID=America/Montreal:20111002T090000
            DTEND;TZID=America/Montreal:20111002T170000
            RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR
            END:AVAILABLE
            END:VAVAILABILITY

        You can get the same example from :meth:`example`:

        .. code-block: pycon

            >>> from icalendar import Availability
            >>> a = Availability.example()
            >>> a.organizer
            vCalAddress('mailto:bernard@example.com')

        The following is an example of a "VAVAILABILITY" calendar
        component used to represent the availability of a user available
        Monday through Thursday, 9:00 am to 5:00 pm, at the main office,
        and Friday, 9:00 am to 12:00 pm, in the branch office in the
        America/Montreal time zone between October 2nd and December 2nd
        2011:

        .. code-block:: text

            BEGIN:VAVAILABILITY
            ORGANIZER:mailto:bernard@example.com
            UID:84D0F948-7FC6-4C1D-BBF3-BA9827B424B5
            DTSTAMP:20111005T133225Z
            DTSTART;TZID=America/Montreal:20111002T000000
            DTEND;TZID=America/Montreal:20111202T000000
            BEGIN:AVAILABLE
            UID:7B33093A-7F98-4EED-B381-A5652530F04D
            SUMMARY:Monday to Thursday from 9:00 to 17:00
            DTSTART;TZID=America/Montreal:20111002T090000
            DTEND;TZID=America/Montreal:20111002T170000
            RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH
            LOCATION:Main Office
            END:AVAILABLE
            BEGIN:AVAILABLE
            UID:DF39DC9E-D8C3-492F-9101-0434E8FC1896
            SUMMARY:Friday from 9:00 to 12:00
            DTSTART;TZID=America/Montreal:20111006T090000
            DTEND;TZID=America/Montreal:20111006T120000
            RRULE:FREQ=WEEKLY
            LOCATION:Branch Office
            END:AVAILABLE
            END:VAVAILABILITY

        For more examples, have a look at :rfc:`5545`.

    """

    name = "VAVAILABILITY"

    canonical_order = (
        "DTSTART",
        "DTEND",
        "DURATION",
        "DTSTAMP",
        "UID",
        "SEQUENCE",
        "SUMMARY",
        "DESCRIPTION",
        "ORGANIZER",
    )

    required = (
        "DTSTART",
        "DTSTAMP",
        "UID",
    )

    singletons = (
        "DTSTAMP",
        "UID",
        "BUSYTYPE",
        "CLASS",
        "CREATED",
        "DESCRIPTION",
        "DTSTART",
        "LAST-MODIFIED",
        "LOCATION",
        "ORGANIZER",
        "PRIORITY",
        "SEQUENCE",
        "SUMMARY",
        "URL",
        "DTEND",
        "DURATION",
    )

    exclusive = (
        "DTEND",
        "DURATION",
    )

    organizer = organizer_property
    busy_type = busy_type_property
    summary = summary_property
    description = description_property
    sequence = sequence_property
    classification = class_property
    url = url_property
    location = location_property
    categories = categories_property
    priority = priority_property
    contacts = contacts_property

    start = DTSTART = rfc_7953_dtstart_property
    DTEND = rfc_7953_dtend_property
    DURATION = duration_property("Availability")
    duration = rfc_7953_duration_property
    end = rfc_7953_end_property

    @property
    def available(self) -> list[Available]:
        """All VAVAILABLE sub-components.

        This is a shortcut to get all VAVAILABLE sub-components.
        Modifications do not change the calendar.
        Use :py:meth:`Component.add_component`.
        """
        return self.walk("AVAILABLE")

    @classmethod
    def new(
        cls,
        /,
        busy_type: BUSYTYPE | None = None,
        categories: Sequence[str] = (),
        comments: list[str] | str | None = None,
        components: Sequence[Available] | None = (),
        concepts: CONCEPTS_TYPE_SETTER = None,
        contacts: list[str] | str | None = None,
        created: date | None = None,
        classification: CLASS | None = None,
        description: str | None = None,
        end: datetime | None = None,
        last_modified: date | None = None,
        links: LINKS_TYPE_SETTER = None,
        location: str | None = None,
        organizer: vCalAddress | str | None = None,
        priority: int | None = None,
        refids: list[str] | str | None = None,
        related_to: RELATED_TO_TYPE_SETTER = None,
        sequence: int | None = None,
        stamp: date | None = None,
        start: datetime | None = None,
        summary: str | None = None,
        uid: str | uuid.UUID | None = None,
        url: str | None = None,
    ):
        """Create a new event with all required properties.

        This creates a new Availability in accordance with :rfc:`7953`.

        Parameters:
            busy_type: The :attr:`busy_type` of the availability.
            categories: The :attr:`categories` of the availability.
            classification: The :attr:`classification` of the availability.
            comments: The :attr:`comments` of the availability.
            concepts: The :attr:`concepts` of the availability.
            contacts: The :attr:`contacts` of the availability.
            created: The :attr:`created` of the availability.
            description: The :attr:`description` of the availability.
            end: The :attr:`end` of the availability.
            last_modified: The :attr:`last_modified` of the
                availability.
            links: The :attr:`links` of the availability.
            location: The :attr:`location` of the availability.
            organizer: The :attr:`organizer` of the availability.
            refids: :attr:`refids` of the availability.
            related_to: :attr:`related_to` of the availability.
            sequence: The :attr:`sequence` of the availability.
            stamp: The :attr:`stamp` of the availability.
                If None, this is set to the current time.
            start: The :attr:`start` of the availability.
            summary: The :attr:`summary` of the availability.
            uid: The :attr:`uid` of the availability.
                If ``None``, this is set to a new :func:`uuid.uuid4`.
            url: The :attr:`url` of the availability.

        Returns:
            :class:`Availability`

        Raises:
            ~error.InvalidCalendar: If the content is not valid
                according to :rfc:`7953`.

        .. warning:: As time progresses, we will be stricter with the validation.
        """
        availability: Availability = super().new(
            stamp=stamp if stamp is not None else cls._utc_now(),
            created=created,
            comments=comments,
            last_modified=last_modified,
            links=links,
            related_to=related_to,
            refids=refids,
            concepts=concepts,
        )
        availability.summary = summary
        availability.description = description
        availability.uid = uid if uid is not None else uuid.uuid4()
        availability.sequence = sequence
        availability.categories = categories
        availability.classification = classification
        availability.url = url
        availability.busy_type = busy_type
        availability.organizer = organizer
        availability.location = location
        availability.priority = priority
        availability.contacts = contacts
        for subcomponent in components:
            availability.add_component(subcomponent)
        if cls._validate_new:
            if start is not None and (
                not isinstance(start, datetime) or start.tzinfo is None
            ):
                raise InvalidCalendar(
                    "Availability start must be a datetime with a timezone"
                )
            if end is not None and (
                not isinstance(end, datetime) or end.tzinfo is None
            ):
                raise InvalidCalendar(
                    "Availability end must be a datetime with a timezone"
                )
            availability._validate_start_and_end(start, end)
        availability.start = start
        availability.end = end
        return availability

    @classmethod
    def example(cls, name: str = "rfc_7953_1") -> Availability:
        """Return the calendar example with the given name."""
        return cls.from_ical(get_example("availabilities", name))


__all__ = ["Availability"]
