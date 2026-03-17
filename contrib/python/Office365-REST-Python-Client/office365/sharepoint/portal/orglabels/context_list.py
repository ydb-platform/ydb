from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.portal.orglabels.context import OrgLabelsContext


class OrgLabelsContextList(ClientValue):
    def __init__(self, is_last_page=None, labels=None):
        self.IsLastPage = is_last_page
        self.Labels = ClientValueCollection(OrgLabelsContext, labels)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.OrgLabelsContextList"
