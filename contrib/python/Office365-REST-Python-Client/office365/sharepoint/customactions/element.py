from office365.runtime.client_value import ClientValue


class CustomActionElement(ClientValue):
    """A class specifies a custom action element."""

    def __init__(self, clientside_component_id=None):
        """
        :param str clientside_component_id: The unique identifier of the client-side component associated
            with the custom action.
        """
        self.ClientSideComponentId = clientside_component_id
