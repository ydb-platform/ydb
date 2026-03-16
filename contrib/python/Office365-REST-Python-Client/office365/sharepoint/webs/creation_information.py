from office365.runtime.client_value import ClientValue


class WebCreationInformation(ClientValue):
    """Represents metadata about site creation."""

    def __init__(self):
        super(WebCreationInformation, self).__init__()
        self.Title = None
        self.Url = None

    @property
    def entity_type_name(self):
        return "SP.WebCreationInformation"
