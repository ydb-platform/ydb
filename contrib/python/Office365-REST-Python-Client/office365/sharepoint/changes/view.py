from typing import Optional

from office365.sharepoint.changes.change import Change


class ChangeView(Change):
    """Specifies a change on a view."""

    @property
    def view_id(self):
        # type: () -> Optional[str]
        """Identifies the changed view."""
        return self.properties.get("ViewId", None)

    @property
    def list_id(self):
        # type: () -> Optional[str]
        """Identifies the list that contains the changed view."""
        return self.properties.get("ListId", None)

    @property
    def web_id(self):
        # type: () -> Optional[str]
        """Identifies the site that contains the changed view."""
        return self.properties.get("WebId", None)
