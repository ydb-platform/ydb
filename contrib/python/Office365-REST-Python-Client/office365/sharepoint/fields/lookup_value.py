from office365.runtime.client_value import ClientValue


class FieldLookupValue(ClientValue):
    def __init__(self, lookup_id=None, lookup_value=None):
        """Specifies the value of a lookup for a fields within a list item.

        :param int lookup_id: Gets or sets the identifier (ID) of the list item that this instance of the lookup
        fields is referring to.
        :param str or None lookup_value: Gets a summary of the list item that this instance
        of the lookup fields is referring to.

        """
        super(FieldLookupValue, self).__init__()
        self.LookupId = lookup_id
        self.LookupValue = lookup_value

    @property
    def entity_type_name(self):
        return "SP.FieldLookupValue"
