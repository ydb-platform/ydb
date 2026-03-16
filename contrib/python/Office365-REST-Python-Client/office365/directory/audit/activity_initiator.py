from office365.directory.applications.app_identity import AppIdentity
from office365.runtime.client_value import ClientValue


class AuditActivityInitiator(ClientValue):
    """
    Identity the resource object that initiates the activity.
    The initiator can be a user, an app, or a system (which is considered an app).
    """

    def __init__(self, app=AppIdentity(), user=AppIdentity()):
        """
        :param AppIdentity app: If the resource initiating the activity is an app, this property indicates all the app
            related information like appId, Name, servicePrincipalId, Name.
        :param AppIdentity user: If the resource initiating the activity is a user, this property Indicates
            all the user related information like userId, Name, UserPrinicpalName.
        """
        self.app = app
        self.user = user
