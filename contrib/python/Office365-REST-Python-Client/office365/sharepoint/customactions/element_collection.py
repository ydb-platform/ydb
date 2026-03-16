from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.customactions.element import CustomActionElement


class CustomActionElementCollection(ClientValue):
    """This is the class that represents a collection of CustomActionElement."""

    def __init__(self):
        super(CustomActionElementCollection, self).__init__()
        self.Items = ClientValueCollection(CustomActionElement)
