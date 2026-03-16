""":rfc:`5545` VFREEBUSY component."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from icalendar.attr import (
    CONCEPTS_TYPE_SETTER,
    LINKS_TYPE_SETTER,
    RELATED_TO_TYPE_SETTER,
    contacts_property,
    create_single_property,
    organizer_property,
    uid_property,
    url_property,
)
from icalendar.cal.component import Component

if TYPE_CHECKING:
    from icalendar.prop import vCalAddress


class FreeBusy(Component):
    """
        A "VFREEBUSY" calendar component is a grouping of component
        properties that represents either a request for free or busy time
        information, a reply to a request for free or busy time
        information, or a published set of busy time information.

    Examples:
        Create a new FreeBusy:

            >>> from icalendar import FreeBusy
            >>> free_busy = FreeBusy.new()
            >>> print(free_busy.to_ical())
            BEGIN:VFREEBUSY
            DTSTAMP:20250517T080612Z
            UID:d755cef5-2311-46ed-a0e1-6733c9e15c63
            END:VFREEBUSY

    """

    name = "VFREEBUSY"

    required = (
        "UID",
        "DTSTAMP",
    )
    singletons = (
        "CONTACT",
        "DTSTART",
        "DTEND",
        "DTSTAMP",
        "ORGANIZER",
        "UID",
        "URL",
    )
    multiple = (
        "ATTENDEE",
        "COMMENT",
        "FREEBUSY",
        "RSTATUS",
    )
    uid = uid_property
    url = url_property
    organizer = organizer_property
    contacts = contacts_property
    start = DTSTART = create_single_property(
        "DTSTART",
        "dt",
        (datetime, date),
        date,
        'The "DTSTART" property for a "VFREEBUSY" specifies the inclusive start of the component.',
    )
    end = DTEND = create_single_property(
        "DTEND",
        "dt",
        (datetime, date),
        date,
        'The "DTEND" property for a "VFREEBUSY" calendar component specifies the non-inclusive end of the component.',
    )

    @property
    def duration(self) -> timedelta | None:
        """The duration computed from start and end."""
        if self.DTSTART is None or self.DTEND is None:
            return None
        return self.DTEND - self.DTSTART

    @classmethod
    def new(
        cls,
        /,
        comments: list[str] | str | None = None,
        concepts: CONCEPTS_TYPE_SETTER = None,
        contacts: list[str] | str | None = None,
        end: date | datetime | None = None,
        links: LINKS_TYPE_SETTER = None,
        organizer: vCalAddress | str | None = None,
        refids: list[str] | str | None = None,
        related_to: RELATED_TO_TYPE_SETTER = None,
        stamp: date | None = None,
        start: date | datetime | None = None,
        uid: str | uuid.UUID | None = None,
        url: str | None = None,
    ):
        """Create a new alarm with all required properties.

        This creates a new Alarm in accordance with :rfc:`5545`.

        Parameters:
            comments: The :attr:`~icalendar.Component.comments` of the component.
            concepts: The :attr:`~icalendar.Component.concepts` of the component.
            contacts: The :attr:`contacts` of the component.
            end: The :attr:`end` of the component.
            links: The :attr:`~icalendar.Component.links` of the component.
            organizer: The :attr:`organizer` of the component.
            refids: :attr:`~icalendar.Component.refids` of the component.
            related_to: :attr:`~icalendar.Component.related_to` of the component.
            stamp: The :attr:`DTSTAMP` of the component.
                If None, this is set to the current time.
            start: The :attr:`start` of the component.
            uid: The :attr:`uid` of the component.
                If None, this is set to a new :func:`uuid.uuid4`.
            url: The :attr:`url` of the component.

        Returns:
            :class:`FreeBusy`

        Raises:
            ~error.InvalidCalendar: If the content is not valid
                according to :rfc:`5545`.

        .. warning:: As time progresses, we will be stricter with the validation.
        """
        free_busy: FreeBusy = super().new(
            stamp=stamp if stamp is not None else cls._utc_now(),
            comments=comments,
            links=links,
            related_to=related_to,
            refids=refids,
            concepts=concepts,
        )
        free_busy.uid = uid if uid is not None else uuid.uuid4()
        free_busy.url = url
        free_busy.organizer = organizer
        free_busy.contacts = contacts
        free_busy.end = end
        free_busy.start = start

        if cls._validate_new:
            cls._validate_start_and_end(start, end)
        return free_busy


__all__ = ["FreeBusy"]
