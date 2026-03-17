from office365.runtime.client_value import ClientValue


class ExtensionSchemaProperty(ClientValue):
    """
    Use the extensionSchemaProperty resource to define a property's name and its type, as part of a schemaExtension
    definition.
    """

    def __init__(self, name=None, type_=None):
        """
        :param str name: The name of the strongly-typed property defined as part of a schema extension.
        :param str type_: The type of the property that is defined as part of a schema extension.
            Allowed values are Binary, Boolean, DateTime, Integer or String. See the table below for more details.
        """
        self.name = name
        self.type = type_
