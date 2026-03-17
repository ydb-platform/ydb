from office365.runtime.client_value import ClientValue


class Identity(ClientValue):
    """The Identity resource represents an identity of an actor. For example, an actor can be a user, device,
    or application."""

    def __init__(self, display_name=None, _id=None):
        """
        :param str display_name: The display name of the identity. Note that this might not always be available or up
            to date. For example, if a user changes their display name, the API might show the new value in a future
            response, but the items associated with the user won't show up as having changed when using delta.

        :param str _id: Unique identifier for the identity.
        """
        super(Identity, self).__init__()
        self.displayName = display_name
        self.id = _id

    def __repr__(self):
        return repr(self.to_json())
