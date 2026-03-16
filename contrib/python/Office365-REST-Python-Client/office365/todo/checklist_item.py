from typing import Optional

from office365.entity import Entity


class ChecklistItem(Entity):
    """Represents a subtask in a bigger todoTask. ChecklistItem allows breaking down a complex task into more
    actionable, smaller tasks."""

    def __str__(self):
        return self.display_name or self.entity_type_name

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """Indicates the title of the checklistItem."""
        return self.properties.get("displayName", None)
