from office365.runtime.client_value import ClientValue


class ResourceReference(ClientValue):
    """Complex type containing properties of officeGraphInsights."""

    def __init__(self, _id=None, _type=None, web_url=None):
        """
        :param str _id: The item's unique identifier.
        :param str _type:A string value that can be used to classify the item, such as "microsoft.graph.driveItem"
        :param str web_url: A URL leading to the referenced item.
        """
        self.id = _id
        self.type = _type
        self.webUrl = web_url

    def __str__(self):
        return self.type or ""

    def __repr__(self):
        return "{0}:{1}".format(self.type, self.webUrl)
