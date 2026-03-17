from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.deleted import DeletedTeam


class Teamwork(Entity):
    """A container for the range of Microsoft Teams functionalities that are available for the organization."""

    @property
    def is_teams_enabled(self):
        # type: () -> Optional[bool]
        """Indicates whether Microsoft Teams is enabled for the organization."""
        return self.properties.get("isTeamsEnabled", None)

    @property
    def region(self):
        # type: () -> Optional[str]
        """Represents the region of the organization or the tenant.
        The region value can be any region supported by the Teams payload"""
        return self.properties.get("region", None)

    @property
    def deleted_teams(self):
        """The tags associated with the team."""
        return self.properties.get(
            "deletedTeams",
            EntityCollection(
                self.context,
                DeletedTeam,
                ResourcePath("deletedTeams", self.resource_path),
            ),
        )
