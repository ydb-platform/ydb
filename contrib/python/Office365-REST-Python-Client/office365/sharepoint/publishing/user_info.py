from office365.runtime.client_value import ClientValue


class UserInfo(ClientValue):
    """Represents user information of a user with consistent color and acronym for client rendering."""

    @property
    def entity_type_name(self):
        return "SP.Publishing.UserInfo"
