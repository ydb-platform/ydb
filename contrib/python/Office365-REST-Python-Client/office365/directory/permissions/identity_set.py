from office365.directory.permissions.identity import Identity
from office365.runtime.client_value import ClientValue


class IdentitySet(ClientValue):
    """The IdentitySet resource is a keyed collection of identity resources. It is used to represent a set of
    identities associated with various events for an item, such as created by or last modified by.
    """

    def __init__(self, application=Identity(), device=Identity(), user=Identity()):
        """
        :param Identity application: The application associated with this action.
        :param Identity device: The device associated with this action.
        :param Identity user: The user associated with this action.
        """
        super(IdentitySet, self).__init__()
        self.application = application
        self.device = device
        self.user = user

    def __repr__(self):
        return repr({n: v.to_json() for n, v in self if v.to_json()})
