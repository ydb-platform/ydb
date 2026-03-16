import uuid

from office365.runtime.client_value_collection import ClientValueCollection


class StringCollection(ClientValueCollection[str]):
    def __init__(self, initial_values=None):
        """
        :type initial_values: list[str] or None
        """
        super(StringCollection, self).__init__(str, initial_values)


class GuidCollection(ClientValueCollection):
    def __init__(self, initial_values=None):
        """
        :type initial_values list[uuid] or None
        """
        super(GuidCollection, self).__init__(uuid.UUID, initial_values)
