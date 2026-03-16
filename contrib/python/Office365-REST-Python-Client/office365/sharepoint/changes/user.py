from typing import Optional

from office365.sharepoint.changes.change import Change


class ChangeUser(Change):
    """Specifies a change on a user."""

    @property
    def activate(self):
        # type: () -> Optional[bool]
        """
        Specifies whether a user has changed from an inactive state to an active state.

        When a user is added to a site and only has browse permissions, the user is in an inactive state.
        However, once the user can author list items, add documents, be assigned tasks, or make any contribution
        to the site, the user is in an active state.
        """
        return self.properties.get("Activate", None)

    @property
    def user_id(self):
        # type: () -> Optional[str]
        """Uniquely identifies the changed user."""
        return self.properties.get("UserId", None)
