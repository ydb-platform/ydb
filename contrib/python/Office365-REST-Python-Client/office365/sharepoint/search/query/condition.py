from office365.runtime.client_value import ClientValue


class QueryCondition(ClientValue):
    """This object contains the conditions for the promoted result"""

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.QueryCondition"
