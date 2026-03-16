from office365.directory.authentication.configuration_base import (
    ApiAuthenticationConfigurationBase,
)


class BasicAuthentication(ApiAuthenticationConfigurationBase):
    """
    Represents configuration for using HTTP Basic authentication, which entails a username and password, in an API call.
     The username and password is sent as the Authorization header as Basic {value} where value is
     base 64 encoded version of username:password.
    """

    def __init__(self, username=None, password=None):
        """
        :param str username: The username.
        :param str password: The password. It is not returned in the responses.
        """
        super(BasicAuthentication, self).__init__()
        self.username = username
        self.password = password
