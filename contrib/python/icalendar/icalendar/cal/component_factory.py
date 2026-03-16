"""A factory to create components."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from icalendar.caselessdict import CaselessDict

if TYPE_CHECKING:
    from icalendar import Component


class ComponentFactory(CaselessDict):
    """Registered components from :rfc:`7953` and :rfc:`5545`.

    To get a component, use this class as shown below.

    .. code-block:: pycon

        >>> from icalendar import ComponentFactory
        >>> factory = ComponentFactory()
        >>> event_class = factory.get_component_class('VEVENT')
        >>> event_class()
        VEVENT({})

    Automatically creates custom component classes for unknown names (X-components,
    IANA-components). Custom components are never dropped per :rfc:`5545`.

    .. code-block:: pycon

        >>> factory = ComponentFactory()
        >>> custom_class = factory.get_component_class('X-VENDOR')
        >>> custom_class()
        X-VENDOR({})

    If a component class is not yet supported, it can be either created
    using :meth:`get_component_class` or added manually as a subclass of
    :class:`~icalendar.cal.component.Component`.
    See :doc:`/how-to/custom-components` for details.
    """

    def __init__(self, *args, **kwargs):
        """Set keys to upper for initial dict."""
        super().__init__(*args, **kwargs)
        from icalendar.cal.alarm import Alarm
        from icalendar.cal.availability import Availability
        from icalendar.cal.available import Available
        from icalendar.cal.calendar import Calendar
        from icalendar.cal.event import Event
        from icalendar.cal.free_busy import FreeBusy
        from icalendar.cal.journal import Journal
        from icalendar.cal.timezone import (
            Timezone,
            TimezoneDaylight,
            TimezoneStandard,
        )
        from icalendar.cal.todo import Todo

        self.add_component_class(Calendar)
        self.add_component_class(Event)
        self.add_component_class(Todo)
        self.add_component_class(Journal)
        self.add_component_class(FreeBusy)
        self.add_component_class(Timezone)
        self.add_component_class(TimezoneStandard)
        self.add_component_class(TimezoneDaylight)
        self.add_component_class(Alarm)
        self.add_component_class(Available)
        self.add_component_class(Availability)

    def add_component_class(self, cls: type[Component]) -> None:
        """Add a component class to the factory.

        Parameters:
            cls: The component class to add.
        """
        self[cls.name] = cls

    def get_component_class(self, name: str) -> type[Component]:
        """Get the component class from the factory.

        This also creates and adds the component class if it does not exist.

        Parameters:
            name: The name of the component, for example, ``"VCALENDAR"``.

        Returns:
            The registered component class.
        """
        component_class = self.get(name)
        if component_class is None:
            from icalendar.cal.component import Component

            component_class = type(
                re.sub(r"[^\w]+", "", name), (Component,), {"name": name.upper()}
            )
            self.add_component_class(component_class)
        return component_class


__all__ = ["ComponentFactory"]
