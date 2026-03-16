from office365.runtime.client_value import ClientValue


class GroupCreationInformation(ClientValue):
    """An object used to facilitate creation of a cross-site group."""

    def __init__(self, title=None, description=None):
        """
        :param str title:
        :param str description:
        """
        self.Title = title
        self.Description = description

    @property
    def entity_type_name(self):
        return "SP.Group"
