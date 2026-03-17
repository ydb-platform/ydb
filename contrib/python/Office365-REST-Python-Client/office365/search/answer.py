from datetime import datetime
from typing import Optional

from office365.entity import Entity
from office365.search.identity_set import IdentitySet


class SearchAnswer(Entity):
    """Represents the base type for other search answers."""

    @property
    def description(self):
        # type: () -> Optional[str]
        """The search answer description that is shown on the search results page."""
        return self.properties.get("description", None)

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The search answer name that is displayed in search results."""
        return self.properties.get("displayName", None)

    @property
    def last_modified_by(self):
        # type: () -> IdentitySet
        """Details of the user who created or last modified the search answer."""
        return self.properties.get("lastModifiedBy", IdentitySet())

    @property
    def last_modified_datetime(self):
        # type: () -> Optional[datetime]
        """Date and time when the search answer was created or last edited."""
        return self.properties.get("lastModifiedDateTime", datetime.min)

    @property
    def web_url(self):
        # type: () -> Optional[str]
        """The URL link for the search answer. When users select this search answer from the search results,
        they are directed to the specified URL."""
        return self.properties.get("webUrl", None)

    @property
    def entity_type_name(self):
        return "microsoft.graph.search.searchAnswer"
