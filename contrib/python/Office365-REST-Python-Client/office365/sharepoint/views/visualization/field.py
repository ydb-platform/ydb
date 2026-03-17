from office365.runtime.client_value import ClientValue


class VisualizationField(ClientValue):
    """Contains CSS properties relating to how an individual field is layed out relative to it's container."""

    def __init__(self, InternalName=None, Style=None):
        """
        :param str InternalName: A Property which will specify which set of sub-elements to apply this set of
            CSS properties on.
        :param str Style: CSS properties in serialized JSON format.
        """
        self.InternalName = InternalName
        self.Style = Style
