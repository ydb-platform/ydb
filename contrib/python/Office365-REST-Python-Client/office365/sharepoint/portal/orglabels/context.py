from office365.runtime.client_value import ClientValue


class OrgLabelsContext(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.OrgLabelsContext"
