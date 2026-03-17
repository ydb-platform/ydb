from office365.runtime.client_value import ClientValue
from office365.sharepoint.search.query.property_value import QueryPropertyValue


class QueryProperty(ClientValue):
    """This object stores additional or custom properties for a search query. A QueryProperty is structured
    as name-value pairs."""

    def __init__(self, name=None, value=QueryPropertyValue()):
        """
        :param str name: This property stores the name part of the QueryProperty name-value pair.
        :param QueryPropertyValue value: This property stores the value part of the QueryProperty name-value pair.
        """
        self.Name = name
        self.Value = value

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.QueryPropertyValue"
