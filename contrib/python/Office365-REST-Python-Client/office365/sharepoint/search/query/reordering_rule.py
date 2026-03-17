from office365.runtime.client_value import ClientValue


class ReorderingRule(ClientValue):
    """The ReorderingRule type contains information about how search results SHOULD be reordered if they met the
    condition."""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.ReorderingRule"
