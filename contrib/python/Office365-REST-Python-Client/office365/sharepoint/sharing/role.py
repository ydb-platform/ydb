from office365.runtime.client_value import ClientValue


class Role(ClientValue):
    """
    Specifies a set of abstract roles that a user can be assigned to share a securable object in a document library
    """

    View = 1
    """The user can only read a securable object."""

    Edit = 2
    """The user can edit or read a securable object, but cannot delete it."""

    Owner = 3
    """The user is an owner of a securable object and can manage permissions, and edit, read or delete the object."""

    @property
    def entity_type_name(self):
        return "SP.Sharing.Role"
