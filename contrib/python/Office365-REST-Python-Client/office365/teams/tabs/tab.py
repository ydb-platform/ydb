from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.apps.app import TeamsApp
from office365.teams.tabs.configuration import TeamsTabConfiguration


class TeamsTab(Entity):
    """
    A teamsTab is a tab that's pinned (attached) to a channel within a team.
    """

    @property
    def teams_app(self):
        """The application that is linked to the tab. This cannot be changed after tab creation."""
        return self.properties.get(
            "teamsApp",
            TeamsApp(self.context, ResourcePath("teamsApp", self.resource_path)),
        )

    @property
    def configuration(self):
        """
        Container for custom settings applied to a tab. The tab is considered configured only once this property is set.
        """
        return self.properties.get("configuration", TeamsTabConfiguration())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "teamsApp": self.teams_app,
            }
            default_value = property_mapping.get(name, None)
        return super(TeamsTab, self).get_property(name, default_value)
