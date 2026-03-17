from office365.runtime.client_value import ClientValue


class Identity(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Comments.Client.Identity"
