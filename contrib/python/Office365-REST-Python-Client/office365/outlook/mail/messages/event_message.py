from office365.outlook.mail.messages.message import Message
from office365.outlook.mail.patterned_recurrence import PatternedRecurrence
from office365.runtime.paths.resource_path import ResourcePath


class EventMessage(Message):
    """A message that represents a meeting request, cancellation, or response (which can be one of the following:
    acceptance, tentative acceptance, or decline)."""

    @property
    def event(self):
        """The event associated with the event message. The assumption for attendees or room resources is that
        the Calendar Attendant is set to automatically update the calendar with an event when meeting request event
        messages arrive. Navigation property. Read-only."""
        from office365.outlook.calendar.events.event import Event

        return self.properties.get(
            "event", Event(self.context, ResourcePath("event", self.resource_path))
        )

    @property
    def patterned_recurrence(self):
        """"""
        return self.properties.get("patternedRecurrence", PatternedRecurrence())
