from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.calendar.calendar import Calendar
from office365.runtime.paths.resource_path import ResourcePath


class CalendarGroup(Entity):
    """
    A group of user calendars.
    """

    @property
    def name(self):
        # type: () -> Optional[str]
        """The group name"""
        return self.properties.get("name", None)

    @property
    def class_id(self):
        # type: () -> Optional[str]
        """The class identifier"""
        return self.properties.get("classId", None)

    @property
    def calendars(self):
        # type: () -> EntityCollection[Calendar]
        """The calendars in the calendar group. Navigation property. Read-only. Nullable."""
        return self.properties.get(
            "calendars",
            EntityCollection(
                self.context, Calendar, ResourcePath("calendars", self.resource_path)
            ),
        )
