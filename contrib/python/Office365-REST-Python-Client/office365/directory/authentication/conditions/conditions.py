from office365.directory.authentication.conditions.applications import (
    AuthenticationConditionsApplications,
)
from office365.runtime.client_value import ClientValue


class AuthenticationConditions(ClientValue):
    """The conditions on which an authenticationEventListener should trigger."""

    def __init__(self, applications=AuthenticationConditionsApplications()):
        """
        :parm AuthenticationConditionsApplications applications: Applications which trigger a custom authentication
        extension.
        """
        self.applications = applications
