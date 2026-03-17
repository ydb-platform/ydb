from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.activities.identity import ActivityIdentity


class GetMentionFacet(ClientValue):
    """"""

    def __init__(self, mentionees=None):
        self.mentionees = ClientValueCollection(ActivityIdentity, mentionees)
