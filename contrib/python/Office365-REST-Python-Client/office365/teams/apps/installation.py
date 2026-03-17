from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.apps.app import TeamsApp
from office365.teams.apps.definition import TeamsAppDefinition


class TeamsAppInstallation(Entity):
    """
    Represents a teamsApp installed in a team or the personal scope of a user. Any bots that are part of the app will
    become part of any team or user's personal scope that the app is added to.
    """

    @property
    def teams_app(self):
        """The app that is installed."""
        return self.properties.get(
            "teamsApp",
            TeamsApp(self.context, ResourcePath("teamsApp", self.resource_path)),
        )

    @property
    def teams_app_definition(self):
        """The details of this version of the app."""
        return self.properties.get(
            "teamsAppDefinition",
            TeamsAppDefinition(
                self.context, ResourcePath("teamsAppDefinition", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "teamsApp": self.teams_app,
                "teamsAppDefinition": self.teams_app_definition,
            }
            default_value = property_mapping.get(name, None)
        return super(TeamsAppInstallation, self).get_property(name, default_value)
