from typing import Optional

from office365.entity_collection import EntityCollection
from office365.outlook.calendar.place import Place
from office365.outlook.calendar.rooms.room import Room
from office365.runtime.paths.resource_path import ResourcePath


class RoomList(Place):
    """Represents a group of room objects defined in the tenant."""

    @property
    def email_address(self):
        # type: () -> Optional[str]
        """The email address of the room list"""
        return self.properties.get("emailAddress", None)

    @property
    def calendars(self):
        # type: () -> EntityCollection[Room]
        """The calendars in the calendar group. Navigation property. Read-only. Nullable."""
        return self.properties.get(
            "rooms",
            EntityCollection(
                self.context, Room, ResourcePath("rooms", self.resource_path)
            ),
        )
