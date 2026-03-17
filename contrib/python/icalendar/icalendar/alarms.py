"""Compute the times and states of alarms.

This takes different calendar software into account and the RFC 9074 (Alarm Extension).

- RFC 9074 defines an ACKNOWLEDGED property in the VALARM.
- Outlook does not export VALARM information.
- Google Calendar uses the DTSTAMP to acknowledge the alarms.
- Thunderbird snoozes the alarms with a X-MOZ-SNOOZE-TIME attribute in the event.
- Thunderbird acknowledges the alarms with a X-MOZ-LASTACK attribute in the event.
- Etar deletes alarms that are acknowledged.
- Nextcloud's Webinterface does not do anything with the alarms when the time passes.
"""

from __future__ import annotations

from datetime import date, timedelta, tzinfo
from typing import TYPE_CHECKING

from icalendar.cal.event import Event
from icalendar.cal.todo import Todo
from icalendar.error import (
    ComponentEndMissing,
    ComponentStartMissing,
    IncompleteAlarmInformation,
    LocalTimezoneMissing,
)
from icalendar.timezone import tzp
from icalendar.tools import is_date, normalize_pytz, to_datetime

if TYPE_CHECKING:
    from collections.abc import Generator
    from datetime import datetime

    from icalendar.cal.alarm import Alarm

Parent = Event | Todo


class AlarmTime:
    """Represents a computed alarm occurrence with its timing and state.

    An AlarmTime instance combines an alarm component with its resolved
    trigger time and additional state information, such as acknowledgment
    and snoozing.
    """

    def __init__(
        self,
        alarm: Alarm,
        trigger: datetime,
        acknowledged_until: datetime | None = None,
        snoozed_until: datetime | None = None,
        parent: Parent | None = None,
    ):
        """Create an instance of ``AlarmTime`` with any of its parameters.

        Parameters:
            alarm: The underlying alarm component.
            trigger: A date or datetime at which to trigger the alarm.
            acknowledged_until: Optional datetime in UTC until which
                the alarm has been acknowledged.
            snoozed_until: Optional datetime in UTC until which
                the alarm has been snoozed.
            parent: Optional parent component to which the alarm refers.
        """
        self._alarm = alarm
        self._parent = parent
        self._trigger = trigger
        self._last_ack = acknowledged_until
        self._snooze_until = snoozed_until

    @property
    def acknowledged(self) -> datetime | None:
        """The time in UTC at which this alarm was last acknowledged.

        If the alarm was not acknowledged (dismissed), then this is None.
        """
        ack = self.alarm.ACKNOWLEDGED
        if ack is None:
            return self._last_ack
        if self._last_ack is None:
            return ack
        return max(ack, self._last_ack)

    @property
    def alarm(self) -> Alarm:
        """The alarm component."""
        return self._alarm

    @property
    def parent(self) -> Parent | None:
        """The component that contains the alarm.

        This is ``None`` if you didn't use :meth:`Alarms.add_component()
        <icalendar.alarms.Alarms.add_component>`.
        """
        return self._parent

    def is_active(self) -> bool:
        """Whether this alarm is active (``True``) or acknowledged (``False``).

        For example, in some calendar software, this is ``True`` until the user
        views the alarm message and dismisses it.

        Alarms can be in local time without a timezone. To calculate whether
        the alarm has occurred, the time must include timezone information.

        Raises:
            LocalTimezoneMissing: If a timezone is required but not given.
        """
        acknowledged = self.acknowledged
        if not acknowledged:
            return True
        if self._snooze_until is not None and self._snooze_until > acknowledged:
            return True
        trigger = self.trigger
        if trigger.tzinfo is None:
            raise LocalTimezoneMissing(
                "A local timezone is required to check if the alarm is still active. "
                "Use Alarms.set_local_timezone()."
            )
        return trigger > acknowledged

    @property
    def trigger(self) -> date:
        """The time at which the alarm triggers.

        If the alarm has been snoozed, this may differ from the TRIGGER property.
        """
        if self._snooze_until is not None and self._snooze_until > self._trigger:
            return self._snooze_until
        return self._trigger


class Alarms:
    """Compute the times and states of alarms.

    This is an example using RFC 9074.
    One alarm is 30 minutes before the event and acknowledged.
    Another alarm is 15 minutes before the event and still active.

    >>> from icalendar import Event, Alarms
    >>> event = Event.from_ical(
    ... '''BEGIN:VEVENT
    ... CREATED:20210301T151004Z
    ... UID:AC67C078-CED3-4BF5-9726-832C3749F627
    ... DTSTAMP:20210301T151004Z
    ... DTSTART;TZID=America/New_York:20210302T103000
    ... DTEND;TZID=America/New_York:20210302T113000
    ... SUMMARY:Meeting
    ... BEGIN:VALARM
    ... UID:8297C37D-BA2D-4476-91AE-C1EAA364F8E1
    ... TRIGGER:-PT30M
    ... ACKNOWLEDGED:20210302T150004Z
    ... DESCRIPTION:Event reminder
    ... ACTION:DISPLAY
    ... END:VALARM
    ... BEGIN:VALARM
    ... UID:8297C37D-BA2D-4476-91AE-C1EAA364F8E1
    ... TRIGGER:-PT15M
    ... DESCRIPTION:Event reminder
    ... ACTION:DISPLAY
    ... END:VALARM
    ... END:VEVENT
    ... ''')
    >>> alarms = Alarms(event)
    >>> len(alarms.times)   # all alarms including those acknowledged
    2
    >>> len(alarms.active)  # the alarms that are not acknowledged, yet
    1
    >>> alarms.active[0].trigger  # this alarm triggers 15 minutes before 10:30
    datetime.datetime(2021, 3, 2, 10, 15, tzinfo=ZoneInfo(key='America/New_York'))

    RFC 9074 specifies that alarms can also be triggered by proximity.
    This is not implemented yet.
    """

    def __init__(self, component: Alarm | Event | Todo | None = None):
        """Start computing alarm times."""
        self._absolute_alarms: list[Alarm] = []
        self._start_alarms: list[Alarm] = []
        self._end_alarms: list[Alarm] = []
        self._start: date | None = None
        self._end: date | None = None
        self._parent: Parent | None = None
        self._last_ack: datetime | None = None
        self._snooze_until: datetime | None = None
        self._local_tzinfo: tzinfo | None = None

        if component is not None:
            self.add_component(component)

    def add_component(self, component: Alarm | Parent):
        """Add a component.

        If this is an alarm, it is added.
        Events and Todos are added as a parent and all
        their alarms are added, too.
        """
        if isinstance(component, (Event, Todo)):
            self.set_parent(component)
            self.set_start(component.start)
            self.set_end(component.end)
            if component.is_thunderbird():
                self.acknowledge_until(component.X_MOZ_LASTACK)
                self.snooze_until(component.X_MOZ_SNOOZE_TIME)
            else:
                self.acknowledge_until(component.DTSTAMP)

        for alarm in component.walk("VALARM"):
            self.add_alarm(alarm)

    def set_parent(self, parent: Parent):
        """Set the parent of all the alarms.

        If you would like to collect alarms from a component, use add_component
        """
        if self._parent is not None and self._parent is not parent:
            raise ValueError("You can only set one parent for this alarm calculation.")
        self._parent = parent

    def add_alarm(self, alarm: Alarm) -> None:
        """Optional: Add an alarm component."""
        trigger = alarm.TRIGGER
        if trigger is None:
            return
        if isinstance(trigger, date):
            self._absolute_alarms.append(alarm)
        elif alarm.TRIGGER_RELATED == "START":
            self._start_alarms.append(alarm)
        else:
            self._end_alarms.append(alarm)

    def set_start(self, dt: date | None):
        """Set the start of the component.

        If you have only absolute alarms, this is not required.
        If you have alarms relative to the start of a compoment, set the start here.
        """
        self._start = dt

    def set_end(self, dt: date | None):
        """Set the end of the component.

        If you have only absolute alarms, this is not required.
        If you have alarms relative to the end of a compoment, set the end here.
        """
        self._end = dt

    def _add(self, dt: date, td: timedelta):
        """Add a timedelta to a datetime."""
        if is_date(dt):
            if td.seconds == 0:
                return dt + td
            dt = to_datetime(dt)
        return normalize_pytz(dt + td)

    def acknowledge_until(self, dt: date | None) -> None:
        """The time in UTC when all the alarms of this component were acknowledged.

        Only the last call counts.

        Since RFC 9074 (Alarm Extension) was created later,
        calendar implementations differ in how they acknowledge alarms.
        For example, Thunderbird and Google Calendar store the last time
        an event has been acknowledged because of an alarm.
        All alarms that happen before this time count as acknowledged.
        """
        self._last_ack = tzp.localize_utc(dt) if dt is not None else None

    def snooze_until(self, dt: date | None) -> None:
        """This is the time in UTC when all the alarms of this component were snoozed.

        Only the last call counts.

        The alarms are supposed to turn up again at dt when they are not acknowledged
        but snoozed.
        """
        self._snooze_until = tzp.localize_utc(dt) if dt is not None else None

    def set_local_timezone(self, tzinfo: tzinfo | str | None):
        """Set the local timezone.

        Events are sometimes in local time.
        In order to compute the exact time of the alarm, some
        alarms without timezone are considered local.

        Some computations work without setting this, others don't.
        If they need this information, expect a LocalTimezoneMissing exception
        somewhere down the line.
        """
        self._local_tzinfo = tzp.timezone(tzinfo) if isinstance(tzinfo, str) else tzinfo

    @property
    def times(self) -> list[AlarmTime]:
        """Compute and return the times of the alarms given.

        If the information for calculation is incomplete, this will raise a
        IncompleteAlarmInformation exception.

        Please make sure to set all the required parameters before calculating.
        If you forget to set the acknowledged times, that is not problem.
        """
        return (
            self._get_end_alarm_times()
            + self._get_start_alarm_times()
            + self._get_absolute_alarm_times()
        )

    def _repeat(self, first: datetime, alarm: Alarm) -> Generator[datetime]:
        """The times when the alarm is triggered relative to start."""
        yield first  # we trigger at the start
        repeat = alarm.REPEAT
        duration = alarm.DURATION
        if repeat and duration:
            for i in range(1, repeat + 1):
                yield self._add(first, duration * i)

    def _alarm_time(self, alarm: Alarm, trigger: date):
        """Create an alarm time with the additional attributes."""
        if getattr(trigger, "tzinfo", None) is None and self._local_tzinfo is not None:
            trigger = normalize_pytz(trigger.replace(tzinfo=self._local_tzinfo))
        return AlarmTime(
            alarm, trigger, self._last_ack, self._snooze_until, self._parent
        )

    def _get_absolute_alarm_times(self) -> list[AlarmTime]:
        """Return a list of absolute alarm times."""
        return [
            self._alarm_time(alarm, trigger)
            for alarm in self._absolute_alarms
            for trigger in self._repeat(alarm.TRIGGER, alarm)
        ]

    def _get_start_alarm_times(self) -> list[AlarmTime]:
        """Return a list of alarm times relative to the start of the component."""
        if self._start is None and self._start_alarms:
            raise ComponentStartMissing(
                "Use Alarms.set_start because at least one alarm is relative to the "
                "start of a component."
            )
        return [
            self._alarm_time(alarm, trigger)
            for alarm in self._start_alarms
            for trigger in self._repeat(self._add(self._start, alarm.TRIGGER), alarm)
        ]

    def _get_end_alarm_times(self) -> list[AlarmTime]:
        """Return a list of alarm times relative to the start of the component."""
        if self._end is None and self._end_alarms:
            raise ComponentEndMissing(
                "Use Alarms.set_end because at least one alarm is relative to the end "
                "of a component."
            )
        return [
            self._alarm_time(alarm, trigger)
            for alarm in self._end_alarms
            for trigger in self._repeat(self._add(self._end, alarm.TRIGGER), alarm)
        ]

    @property
    def active(self) -> list[AlarmTime]:
        """The alarm times that are still active and not acknowledged.

        This considers snoozed alarms.

        Alarms can be in local time (without a timezone).
        To calculate if the alarm really happened, we need it to be in a timezone.
        If a timezone is required but not given, we throw an IncompleteAlarmInformation.
        """
        return [alarm_time for alarm_time in self.times if alarm_time.is_active()]


__all__ = [
    "AlarmTime",
    "Alarms",
    "ComponentEndMissing",
    "ComponentStartMissing",
    "IncompleteAlarmInformation",
]
