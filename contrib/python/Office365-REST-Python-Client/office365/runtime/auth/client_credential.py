class ClientCredential(object):
    def __init__(self, client_id, client_secret):
        """
        Client credentials

        :param str client_secret:
        :param str client_id:
        """
        self.clientId = client_id
        self.clientSecret = client_secret
