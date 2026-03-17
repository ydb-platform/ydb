from office365.runtime.client_value import ClientValue


class ContentTypeId(ClientValue):
    """
    The ContentTypeId type is the identifier for the specified content type. The identifier is a string of
    hexadecimal characters. The identifier MUST be unique relative to the current site collection and site and MUST
    follow the pattern of prefixing a ContentTypeId with its parent ContentTypeId.

    ContentTypeId MUST follow the XSD pattern specified in [MS-WSSCAML] section 2.3.1.4.
    """

    def __init__(self, string_value=None):
        """
        Represents the content type identifier (ID) of a content type.

        :param str string_value: Hexadecimal string value of content type identifier. String value MUST start with "0x".
        """
        super(ContentTypeId, self).__init__()
        self.StringValue = string_value

    def __repr__(self):
        return self.StringValue or self.entity_type_name
