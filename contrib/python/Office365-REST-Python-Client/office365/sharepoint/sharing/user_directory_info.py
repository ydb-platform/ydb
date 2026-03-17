from office365.runtime.client_value import ClientValue


class UserDirectoryInfo(ClientValue):
    """User information from directory service."""

    def __init__(self, name=None, net_id=None, primary_email=None, principal_name=None):
        """
        :param str name: Principal name of the directory user. E.g. user@domain.com.
        :param str net_id: User NetId property in directory.
        :param str primary_email: User primary email of the directory user. E.g. user@domain.com.
        :param str principal_name: Principal name of the directory user. E.g. user@domain.com.
        """
        super(UserDirectoryInfo, self).__init__()
        self.Name = name
        self.NetId = net_id
        self.PrimaryEmail = primary_email
        self.PrincipalName = principal_name

    @property
    def entity_type_name(self):
        return "SP.Sharing.UserDirectoryInfo"
