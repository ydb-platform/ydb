""":rfc:`5545` VALARM component."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, NamedTuple

from icalendar.attr import (
    CONCEPTS_TYPE_SETTER,
    LINKS_TYPE_SETTER,
    RELATED_TO_TYPE_SETTER,
    attendees_property,
    create_single_property,
    description_property,
    property_del_duration,
    property_get_duration,
    property_set_duration,
    single_int_property,
    single_string_property,
    single_utc_property,
    summary_property,
    uid_property,
)
from icalendar.cal.component import Component
from icalendar.cal.examples import get_example

if TYPE_CHECKING:
    import uuid

    from icalendar.prop import vCalAddress


class Alarm(Component):
    """
    A "VALARM" calendar component is a grouping of component
    properties that defines an alarm or reminder for an event or a
    to-do. For example, it may be used to define a reminder for a
    pending event or an overdue to-do.

    Example:

        The following example creates an alarm which uses an audio file
        from an FTP server.

        .. code-block:: pycon

            >>> from icalendar import Alarm
            >>> alarm = Alarm.example()
            >>> print(alarm.to_ical().decode())
            BEGIN:VALARM
            ACTION:AUDIO
            ATTACH;FMTTYPE=audio/basic:ftp://example.com/pub/sounds/bell-01.aud
            DURATION:PT15M
            REPEAT:4
            TRIGGER;VALUE=DATE-TIME:19970317T133000Z
            END:VALARM
    """

    name = "VALARM"
    # some properties MAY/MUST/MUST NOT appear depending on ACTION value
    required = (
        "ACTION",
        "TRIGGER",
    )
    singletons = (
        "ATTACH",
        "ACTION",
        "DESCRIPTION",
        "SUMMARY",
        "TRIGGER",
        "DURATION",
        "REPEAT",
        "UID",
        "PROXIMITY",
        "ACKNOWLEDGED",
    )
    inclusive = (
        (
            "DURATION",
            "REPEAT",
        ),
        (
            "SUMMARY",
            "ATTENDEE",
        ),
    )
    multiple = ("ATTENDEE", "ATTACH", "RELATED-TO")

    REPEAT = single_int_property(
        "REPEAT",
        0,
        """The REPEAT property of an alarm component.

        The alarm can be defined such that it triggers repeatedly.  A
        definition of an alarm with a repeating trigger MUST include both
        the "DURATION" and "REPEAT" properties.  The "DURATION" property
        specifies the delay period, after which the alarm will repeat.
        The "REPEAT" property specifies the number of additional
        repetitions that the alarm will be triggered.  This repetition
        count is in addition to the initial triggering of the alarm.
        """,
    )

    DURATION = property(
        property_get_duration,
        property_set_duration,
        property_del_duration,
        """The DURATION property of an alarm component.

    The alarm can be defined such that it triggers repeatedly.  A
    definition of an alarm with a repeating trigger MUST include both
    the "DURATION" and "REPEAT" properties.  The "DURATION" property
    specifies the delay period, after which the alarm will repeat.
    """,
    )

    ACKNOWLEDGED = single_utc_property(
        "ACKNOWLEDGED",
        """This is defined in RFC 9074:

    Purpose: This property specifies the UTC date and time at which the
    corresponding alarm was last sent or acknowledged.

    This property is used to specify when an alarm was last sent or acknowledged.
    This allows clients to determine when a pending alarm has been acknowledged
    by a calendar user so that any alerts can be dismissed across multiple devices.
    It also allows clients to track repeating alarms or alarms on recurring events or
    to-dos to ensure that the right number of missed alarms can be tracked.

    Clients SHOULD set this property to the current date-time value in UTC
    when a calendar user acknowledges a pending alarm. Certain kinds of alarms,
    such as email-based alerts, might not provide feedback as to when the calendar user
    sees them. For those kinds of alarms, the client SHOULD set this property
    when the alarm is triggered and the action is successfully carried out.

    When an alarm is triggered on a client, clients can check to see if an "ACKNOWLEDGED"
    property is present. If it is, and the value of that property is greater than or
    equal to the computed trigger time for the alarm, then the client SHOULD NOT trigger
    the alarm. Similarly, if an alarm has been triggered and
    an "alert" has been presented to a calendar user, clients can monitor
    the iCalendar data to determine whether an "ACKNOWLEDGED" property is added or
    changed in the alarm component. If the value of any "ACKNOWLEDGED" property
    in the alarm changes and is greater than or equal to the trigger time of the alarm,
    then clients SHOULD dismiss or cancel any "alert" presented to the calendar user.
    """,
    )

    TRIGGER = create_single_property(
        "TRIGGER",
        "dt",
        (datetime, timedelta),
        timedelta | datetime | None,
        """Purpose:  This property specifies when an alarm will trigger.

    Value Type:  The default value type is DURATION.  The value type can
    be set to a DATE-TIME value type, in which case the value MUST
    specify a UTC-formatted DATE-TIME value.

    Either a positive or negative duration may be specified for the
    "TRIGGER" property.  An alarm with a positive duration is
    triggered after the associated start or end of the event or to-do.
    An alarm with a negative duration is triggered before the
    associated start or end of the event or to-do.""",
    )

    @property
    def TRIGGER_RELATED(self) -> str:
        """The RELATED parameter of the TRIGGER property.

        Values are either "START" (default) or "END".

        A value of START will set the alarm to trigger off the
        start of the associated event or to-do.  A value of END will set
        the alarm to trigger off the end of the associated event or to-do.

        In this example, we create an alarm that triggers two hours after the
        end of its parent component:

        >>> from icalendar import Alarm
        >>> from datetime import timedelta
        >>> alarm = Alarm()
        >>> alarm.TRIGGER = timedelta(hours=2)
        >>> alarm.TRIGGER_RELATED = "END"
        """
        trigger = self.get("TRIGGER")
        if trigger is None:
            return "START"
        return trigger.params.get("RELATED", "START")

    @TRIGGER_RELATED.setter
    def TRIGGER_RELATED(self, value: str):
        """Set "START" or "END"."""
        trigger = self.get("TRIGGER")
        if trigger is None:
            raise ValueError(
                "You must set a TRIGGER before setting the RELATED parameter."
            )
        trigger.params["RELATED"] = value

    class Triggers(NamedTuple):
        """The computed times of alarm triggers.

        start - triggers relative to the start of the Event or Todo (timedelta)

        end - triggers relative to the end of the Event or Todo (timedelta)

        absolute - triggers at a datetime in UTC
        """

        start: tuple[timedelta]
        end: tuple[timedelta]
        absolute: tuple[datetime]

    @property
    def triggers(self):
        """The computed triggers of an Alarm.

        This takes the TRIGGER, DURATION and REPEAT properties into account.

        Here, we create an alarm that triggers 3 times before the start of the
        parent component:

        >>> from icalendar import Alarm
        >>> from datetime import timedelta
        >>> alarm = Alarm()
        >>> alarm.TRIGGER = timedelta(hours=-4)  # trigger 4 hours before START
        >>> alarm.DURATION = timedelta(hours=1)  # after 1 hour trigger again
        >>> alarm.REPEAT = 2  # trigger 2 more times
        >>> alarm.triggers.start == (timedelta(hours=-4),  timedelta(hours=-3),  timedelta(hours=-2))
        True
        >>> alarm.triggers.end
        ()
        >>> alarm.triggers.absolute
        ()
        """
        start = []
        end = []
        absolute = []
        trigger = self.TRIGGER
        if trigger is not None:
            if isinstance(trigger, date):
                absolute.append(trigger)
                add = absolute
            elif self.TRIGGER_RELATED == "START":
                start.append(trigger)
                add = start
            else:
                end.append(trigger)
                add = end
            duration = self.DURATION
            if duration is not None:
                for _ in range(self.REPEAT):
                    add.append(add[-1] + duration)
        return self.Triggers(
            start=tuple(start), end=tuple(end), absolute=tuple(absolute)
        )

    uid = single_string_property(
        "UID",
        uid_property.__doc__,
        "X-ALARMUID",
    )
    summary = summary_property
    description = description_property
    attendees = attendees_property

    @classmethod
    def new(
        cls,
        /,
        attendees: list[vCalAddress] | None = None,
        concepts: CONCEPTS_TYPE_SETTER = None,
        description: str | None = None,
        links: LINKS_TYPE_SETTER = None,
        refids: list[str] | str | None = None,
        related_to: RELATED_TO_TYPE_SETTER = None,
        summary: str | None = None,
        uid: str | uuid.UUID | None = None,
    ):
        """Create a new alarm with all required properties.

        This creates a new Alarm in accordance with :rfc:`5545`.

        Parameters:
            attendees: The :attr:`attendees` of the alarm.
            concepts: The :attr:`~icalendar.Component.concepts` of the alarm.
            description: The :attr:`description` of the alarm.
            links: The :attr:`~icalendar.Component.links` of the alarm.
            refids: :attr:`~icalendar.Component.refids` of the alarm.
            related_to: :attr:`~icalendar.Component.related_to` of the alarm.
            summary: The :attr:`summary` of the alarm.
            uid: The :attr:`uid` of the alarm.

        Returns:
            :class:`Alarm`

        Raises:
            ~error.InvalidCalendar: If the content is not valid
                according to :rfc:`5545`.

        .. warning:: As time progresses, we will be stricter with the validation.
        """
        alarm: Alarm = super().new(
            links=links,
            related_to=related_to,
            refids=refids,
            concepts=concepts,
        )
        alarm.summary = summary
        alarm.description = description
        alarm.uid = uid
        alarm.attendees = attendees
        return alarm

    @classmethod
    def example(cls, name: str = "example") -> Alarm:
        """Return the alarm example with the given name."""
        return cls.from_ical(get_example("alarms", name))


__all__ = ["Alarm"]
