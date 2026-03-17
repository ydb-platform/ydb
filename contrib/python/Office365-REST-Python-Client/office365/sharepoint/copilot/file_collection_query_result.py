from office365.runtime.client_value import ClientValue


class CopilotFileCollectionQueryResult(ClientValue):
    """ """

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.SharePoint.Copilot.CopilotFileCollectionQueryResult"
