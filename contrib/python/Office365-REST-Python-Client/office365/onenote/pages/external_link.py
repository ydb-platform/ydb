from office365.runtime.client_value import ClientValue


class ExternalLink(ClientValue):
    """Represents a URL that opens a OneNote page or notebook."""

    def __init__(self, href=None):
        """
        :param str href: The URL of the link.
        """
        self.href = href

    def __repr__(self):
        return self.href
