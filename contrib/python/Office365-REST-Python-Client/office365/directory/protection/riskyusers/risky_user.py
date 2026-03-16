from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class RiskyUser(Entity):
    """
    Represents Azure AD users who are at risk. Azure AD continually evaluates user risk based on various
    signals and machine learning. This API provides programmatic access to all at-risk users in your Azure AD.
    """

    @property
    def history(self):
        """The activity related to user risk level change"""
        from office365.directory.protection.riskyusers.history_item import (
            RiskyUserHistoryItem,
        )

        return self.properties.get(
            "history",
            EntityCollection(
                self.context,
                RiskyUserHistoryItem,
                ResourcePath("history", self.resource_path),
            ),
        )

    @property
    def user_principal_name(self):
        # type: () -> Optional[str]
        """Risky user principal name."""
        return self.properties.get("userPrincipalName", None)
