from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.activities.identity import ActivityIdentity


class SharingFacet(ClientValue):
    def __init__(self, recipients=None, sharing_type=None):
        """
        :param list[ActivityIdentity] recipients:
        :param str sharing_type:
        """
        self.recipients = ClientValueCollection(ActivityIdentity, recipients)
        self.sharingType = sharing_type
