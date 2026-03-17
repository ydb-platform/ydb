from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class GetRemoteItemInfoRequest(ClientValue):
    def __init__(self, RemoteItemUniqueIds=None):
        self.RemoteItemUniqueIds = StringCollection(RemoteItemUniqueIds)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.AddToOneDrive.GetRemoteItemInfoRequest"
