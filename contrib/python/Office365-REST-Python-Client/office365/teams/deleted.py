from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.channels.collection import ChannelCollection


class DeletedTeam(Entity):
    """
    A deleted team in Microsoft Teams is a collection of channel objects. A channel represents a topic,
    and therefore a logical isolation of discussion, within a deleted team.

    Every deleted team is associated with a Microsoft 365 group. For more information about working with groups
    and members in teams, see Use the Microsoft Graph REST API to work with Microsoft Teams.
    """

    @property
    def channels(self):
        # type: () -> ChannelCollection
        """The collection of channels & messages associated with the team."""
        return self.properties.get(
            "channels",
            ChannelCollection(
                self.context, ResourcePath("channels", self.resource_path)
            ),
        )
