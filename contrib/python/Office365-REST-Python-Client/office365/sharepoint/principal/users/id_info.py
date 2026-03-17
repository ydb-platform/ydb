from office365.runtime.client_value import ClientValue


class UserIdInfo(ClientValue):
    def __init__(self, name_id=None, name_id_issuer=None):
        """Represents an identity providers unique identifier information

        :param str name_id: Specifies the identity provider's unique identifier.
        :param str name_id_issuer: Specifies the identity provider's display name as registered in a farm.
        """
        super(UserIdInfo, self).__init__()
        self.NameId = name_id
        self.NameIdIssuer = name_id_issuer
