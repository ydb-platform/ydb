from office365.runtime.client_value import ClientValue


class XmlSchemaFieldCreationInformation(ClientValue):
    def __init__(self, schema_xml=None, options=None):
        """
        Specifies metadata about a field

        :param str schema_xml: Specifies the schema that defines the field
        :param int or None options: Specifies the control settings that are used while adding a field
        """
        super(XmlSchemaFieldCreationInformation, self).__init__()
        self.SchemaXml = schema_xml
        self.Options = options

    @property
    def entity_type_name(self):
        return "SP.XmlSchemaFieldCreationInformation"
