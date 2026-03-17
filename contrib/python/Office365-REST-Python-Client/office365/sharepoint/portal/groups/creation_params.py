from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class GroupCreationParams(ClientValue):
    def __init__(self, classification="", description=""):
        super(GroupCreationParams, self).__init__()
        self.Classification = classification
        self.Description = description
        self.CreationOptions = ClientValueCollection(str)
        self.CreationOptions.add("SPSiteLanguage:1033")

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.GroupCreationParams"
