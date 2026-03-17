from office365.runtime.client_value import ClientValue


class FieldUrlValue(ClientValue):
    """Specifies the hyperlink and the description values for FieldURL."""

    def __init__(self, url=None, description=None):
        """
        :param str url: Specifies the URI. Its length MUST be equal to or less than 255. It MUST be one
             of the following: NULL, empty, an absolute URL, or a server-relative URL.
        :param str description: Specifies the description for the URI. Its length MUST be equal to or less than 255.
        """
        super(FieldUrlValue, self).__init__()
        self.Url = url
        self.Description = description

    @property
    def entity_type_name(self):
        return "SP.FieldUrlValue"
