from office365.runtime.client_value import ClientValue


class ExpandedQueryParameters(ClientValue):
    """This object contains the dictionary of the expanded query parameters."""

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ExpandedQueryParameters"
