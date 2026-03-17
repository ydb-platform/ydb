from office365.runtime.client_value import ClientValue


class BaseGptResponse(ClientValue):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Internal.BaseGptResponse"
