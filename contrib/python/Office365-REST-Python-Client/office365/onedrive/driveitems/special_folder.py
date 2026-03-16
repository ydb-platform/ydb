from office365.runtime.client_value import ClientValue


class SpecialFolder(ClientValue):
    """The SpecialFolder resource groups special folder-related data items into a single structure."""

    def __init__(self, name=None):
        """
        :param str name:
        """
        self.name = name
