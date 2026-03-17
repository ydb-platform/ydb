from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.apps.installation import TeamsAppInstallation
from office365.teams.chats.chat import Chat


class UserScopeTeamsAppInstallation(TeamsAppInstallation):
    """
    Represents a teamsApp installed in the personal scope of a user. Any bots that are part of the app will become
    part of a user's personal scope that the app is added to. This type inherits from teamsAppInstallation.
    """

    @property
    def chat(self):
        """
        The chat between the user and Teams app.
        """
        return self.properties.get(
            "chat", Chat(self.context, ResourcePath("chat", self.resource_path))
        )
