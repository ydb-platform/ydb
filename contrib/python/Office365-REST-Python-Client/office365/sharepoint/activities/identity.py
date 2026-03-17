from office365.runtime.client_value import ClientValue
from office365.sharepoint.activities.identity_item import ActivityIdentityItem


class ActivityIdentity(ClientValue):
    def __init__(
        self, client_id=None, group=ActivityIdentityItem(), user=ActivityIdentityItem()
    ):
        """
        :param str client_id:
        """
        self.clientId = client_id
        self.group = group
        self.user = user

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.ActivityIdentity"
