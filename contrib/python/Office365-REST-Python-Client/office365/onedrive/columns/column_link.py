from typing import Optional

from office365.entity import Entity


class ColumnLink(Entity):
    """A columnLink on a contentType attaches a site columnDefinition to that content type."""

    @property
    def name(self):
        # type: () -> Optional[str]
        """The name of the column in this content type."""
        return self.properties.get("name", None)
