import datetime

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.bots.teamwork_bot import TeamworkBot


class TeamsAppDefinition(Entity):
    """Represents the details of a version of a teamsApp."""

    @property
    def bot(self):
        """The details of the bot specified in the Teams app manifest."""
        return self.properties.get(
            "bot", TeamworkBot(self.context, ResourcePath("bot", self.resource_path))
        )

    @property
    def created_by(self):
        """Identity of the user, device, or application which created the item."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def description(self):
        """Verbose description of the application."""
        return self.properties.get("description", IdentitySet())

    @property
    def last_modified_datetime(self):
        """Gets date and time the teamsApp was last modified."""
        return self.properties.get("lastModifiedDateTime", datetime.datetime.min)

    @property
    def teams_app_id(self):
        """The ID from the Teams app manifest."""
        return self.properties.get("teamsAppId", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
                "lastModifiedDateTime": self.last_modified_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(TeamsAppDefinition, self).get_property(name, default_value)
