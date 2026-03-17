from office365.runtime.client_value import ClientValue


class Sort(ClientValue):
    """Contains information about the property to sort the search results on, and how to sort on the property."""

    def __init__(self, property_name=None, direction=None):
        """
        :param str property_name: If direction is equal to SortDirection.Ascending or SortDirection.Descending,
            then this element specifies the name of the managed property to sort the search results on
        :param int direction: The direction in which to sort on the property specified in the property_name element
        """
        self.Direction = direction
        self.Property = property_name

    def __str__(self):
        return "{0}:{1}".format(self.Property, self.Direction)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.Sort"
