from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.apps.app import TeamsApp


class AppCatalogs(Entity):
    """A container for apps from the Microsoft Teams app catalog"""

    @property
    def teams_apps(self):
        # type: () -> EntityCollection[TeamsApp]
        """List apps from the Microsoft Teams app catalog."""
        return self.properties.get(
            "teamsApps",
            EntityCollection(
                self.context, TeamsApp, ResourcePath("teamsApps", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"teamsApps": self.teams_apps}
            default_value = property_mapping.get(name, None)
        return super(AppCatalogs, self).get_property(name, default_value)
