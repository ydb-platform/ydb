from office365.runtime.client_value import ClientValue


class ListHomeItem(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.ListHome.ListHomeItem"
