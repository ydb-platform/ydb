from office365.runtime.client_value import ClientValue


class ContentTypeOrder(ClientValue):
    """The contentTypeOrder resource specifies in which order the Content Type will appear in the selection UI."""

    def __init__(self, default=None, position=None):
        """
        :param bool default:  Whether this is the default Content Type
        :param str position:  Specifies the position in which the Content Type appears in the selection UI.
        """
        self.default = default
        self.position = position
