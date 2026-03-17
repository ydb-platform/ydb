import datetime
from typing import List

from office365.delta_collection import DeltaCollection
from office365.outlook.calendar.attendees.attendee import Attendee
from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.outlook.calendar.email_address import EmailAddress
from office365.outlook.calendar.events.event import Event
from office365.outlook.mail.item_body import ItemBody
from office365.runtime.client_value_collection import ClientValueCollection


class EventCollection(DeltaCollection[Event]):
    def __init__(self, context, resource_path=None):
        super(EventCollection, self).__init__(context, Event, resource_path)

    def add(
        self, subject=None, body=None, start=None, end=None, attendees=None, **kwargs
    ):
        # type: (str, str|ItemBody, datetime.datetime, datetime.datetime, List[str], ...) -> Event
        """
        Create an event in the user's default calendar or specified calendar.

        By default, the allowNewTimeProposals property is set to true when an event is created,
        which means invitees can propose a different date/time for the event. See Propose new meeting times
        for more information on how to propose a time, and how to receive and accept a new time proposal.

        :param str subject: The subject of the message.
        :param str or ItemBody body: The body of the message. It can be in HTML or text format
        :param datetime.datetime start: The start date, time, and time zone of the event.
             By default, the start time is in UTC.
        :param datetime.datetime end: The date, time, and time zone that the event ends.
            By default, the end time is in UTC.
        :param list[str] attendees: The collection of attendees for the event.
        """

        if body is not None:
            kwargs["body"] = body if isinstance(body, ItemBody) else ItemBody(body)
        if subject is not None:
            kwargs["subject"] = subject
        if start is not None:
            kwargs["start"] = DateTimeTimeZone.parse(start)
        if end is not None:
            kwargs["end"] = DateTimeTimeZone.parse(end)

        if attendees is not None:
            kwargs["attendees"] = ClientValueCollection(
                Attendee,
                [
                    Attendee(EmailAddress(v), attendee_type="required")
                    for v in attendees
                ],
            )

        return super(EventCollection, self).add(**kwargs)
