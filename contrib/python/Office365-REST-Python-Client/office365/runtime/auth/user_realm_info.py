class UserRealmInfo(object):
    def __init__(self, auth_url, federated):
        """

        :type federated: bool
        :type auth_url: str or None
        """
        self.STSAuthUrl = auth_url
        self.IsFederated = federated
