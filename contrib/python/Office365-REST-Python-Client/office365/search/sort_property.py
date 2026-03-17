from office365.runtime.client_value import ClientValue


class SortProperty(ClientValue):
    """Indicates the order to sort search results."""

    def __init__(self, is_descending=None, name=None):
        """
        :param bool is_descending: True if the sort order is descending. Default is false,
             with the sort order as ascending. Optional.
        :param str name: The name of the property to sort on. Required.
        """
        self.isDescending = is_descending
        self.name = name

    def __repr__(self):
        return "{0} {1}".format(self.name, "DESC" if self.isDescending else "ASC")
