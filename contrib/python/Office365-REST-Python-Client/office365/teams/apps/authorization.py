from office365.runtime.client_value import ClientValue
from office365.teams.apps.permission_set import TeamsAppPermissionSet


class TeamsAppAuthorization(ClientValue):
    """The authorization details of a teamsApp."""

    def __init__(
        self, client_app_id=None, required_permission_set=TeamsAppPermissionSet()
    ):
        """
        :param str client_app_id: 	The registration ID of the Microsoft Entra app ID associated with the teamsApp.
        :param TeamsAppPermissionSet required_permission_set: Set of permissions required by the teamsApp.
        """
        self.clientAppId = client_app_id
        self.requiredPermissionSet = required_permission_set
