from office365.runtime.client_value import ClientValue


class SharedWithMeViewItemRemovalResult(ClientValue):
    """An object that contains the result of calling the API to remove an item from a user's 'Shared With Me' view."""

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharedWithMeViewItemRemovalResult"
