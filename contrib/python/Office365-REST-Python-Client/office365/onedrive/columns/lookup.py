from office365.runtime.client_value import ClientValue


class LookupColumn(ClientValue):
    """
    The lookupColumn on a columnDefinition resource indicates that the column's values
    are looked up from another source in the site.
    """

    def __init__(
        self,
        list_id=None,
        column_name=None,
        allow_multiple_values=None,
        allow_unlimited_length=None,
        primary_lookup_column_id=None,
    ):
        """
        :param str list_id: The unique identifier of the lookup source list.
        :param str column_name: The name of the lookup source column.
        :param bool allow_multiple_values: Indicates whether multiple values can be selected from the source.
        :param bool allow_unlimited_length: Indicates whether values in the column should be able to exceed
            the standard limit of 255 characters.
        :param str primary_lookup_column_id: If specified, this column is a secondary lookup, pulling an additional
            field from the list item looked up by the primary lookup. Use the list item looked up by the primary
            as the source for the column named here.
        """
        self.listId = list_id
        self.columnName = column_name
        self.allowMultipleValues = allow_multiple_values
        self.allowUnlimitedLength = allow_unlimited_length
        self.primaryLookupColumnId = primary_lookup_column_id
