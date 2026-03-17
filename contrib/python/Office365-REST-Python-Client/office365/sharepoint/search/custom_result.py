from office365.runtime.client_value import ClientValue


class CustomResult(ClientValue):
    """
    This contains a list of query results, all of which are of the type specified in TableType.
    """

    def __init__(
        self,
        group_template_id=None,
        item_template_id=None,
        result_title=None,
        properties=None,
    ):
        """
        :param str group_template_id: Specifies the identifier of the layout template that specifies how the results
            returned will be arranged.
        :param str item_template_id: Specifies the identifier of the layout template that specifies how the result
            item will be displayed.
        :param str result_title: Specifies the title associated with results for the transformed query by query rule
            action. MUST NOT be more than 64 characters in length.
        :param dict properties: Specifies a property bag of key value pairs.
        """
        self.GroupTemplateId = group_template_id
        self.ItemTemplateId = item_template_id
        self.Properties = properties
        self.ResultTitle = result_title

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.CustomResult"
