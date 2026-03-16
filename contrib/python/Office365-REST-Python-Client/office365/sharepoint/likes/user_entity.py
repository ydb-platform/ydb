from typing import Optional

from office365.sharepoint.entity import Entity


class UserEntity(Entity):
    """Represents a single like within a likedBy set of the list item."""

    @property
    def creation_date(self):
        # type: () -> Optional[str]
        """The Datetime of the like."""
        return self.properties.get("creationDate", None)

    @property
    def email(self):
        # type: () -> Optional[str]
        """The email of the user who liked the item."""
        return self.properties.get("email", None)
