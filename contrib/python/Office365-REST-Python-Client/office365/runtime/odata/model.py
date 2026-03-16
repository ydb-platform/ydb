class ODataModel(object):
    """OData model"""

    _types = {}

    @property
    def types(self):
        return self._types

    def add_type(self, type_schema):
        """
        :type type_schema: office365.runtime.odata.type.ODataType
        :return:
        """
        type_alias = type_schema.name
        self._types[type_alias] = type_schema
