from office365.runtime.client_value import ClientValue


class QueryPropertyValue(ClientValue):
    """This object is used to store values of predefined types. The object MUST have a value set for only
    one of the property."""

    def __init__(
        self,
        bool_val=None,
        int_val=None,
        str_array=None,
        str_val=None,
        query_property_value_type_index=None,
    ):
        """
        :param bool bool_val: Specifies any arbitrary value of type CSOM Boolean. This property MUST have a value only
            if QueryPropertyValueTypeIndex is set to 3.
        :param int int_val: Specifies any arbitrary value of type CSOM Int32. This property MUST have a value only
            if QueryPropertyValueTypeIndex is set to 2.
        :param int query_property_value_type_index: Specifies the type of data stored in this object.
        :param list[str] str_array: Specifies any arbitrary value of type CSOM array. This property MUST have a
             value only if QueryPropertyValueTypeIndex is set to 4.
        :param str str_val: Specifies any arbitrary value of type CSOM String. This property MUST have a value
             only if QueryPropertyValueTypeIndex is set to 1.
        """
        self.BoolVal = bool_val
        self.IntVal = int_val
        self.StrArray = str_array
        self.StrVal = str_val
        self.QueryPropertyValueTypeIndex = query_property_value_type_index

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.QueryPropertyValue"
