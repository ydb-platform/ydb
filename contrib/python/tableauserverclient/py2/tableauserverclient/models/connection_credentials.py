from .property_decorators import property_is_boolean


class ConnectionCredentials(object):
    """Connection Credentials for Workbooks and Datasources publish request.

    Consider removing this object and other variables holding secrets
    as soon as possible after use to avoid them hanging around in memory.

    """

    def __init__(self, name, password, embed=True, oauth=False):
        self.name = name
        self.password = password
        self.embed = embed
        self.oauth = oauth

    @property
    def embed(self):
        return self._embed

    @embed.setter
    @property_is_boolean
    def embed(self, value):
        self._embed = value

    @property
    def oauth(self):
        return self._oauth

    @oauth.setter
    @property_is_boolean
    def oauth(self, value):
        self._oauth = value

    @classmethod
    def from_xml_element(cls, parsed_response, ns):
        connection_creds_xml = parsed_response.find('.//t:connectionCredentials', namespaces=ns)

        name = connection_creds_xml.get('name', None)
        password = connection_creds_xml.get('password', None)
        embed = connection_creds_xml.get('embed', None)
        oAuth = connection_creds_xml.get('oAuth', None)

        connection_creds = cls(name, password, embed, oAuth)
        return connection_creds
