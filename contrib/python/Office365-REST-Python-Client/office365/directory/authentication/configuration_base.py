from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class ApiAuthenticationConfigurationBase(ClientValue):
    """
    The base type to hold authentication information for calling an API.

    Derived types include:

    - basicAuthentication for HTTP basic authentication
    - pkcs12certificate for client certificate authentication (used for API connector create or upload)
    - clientCertificateAuthentication for client certificate authentication (used for fetching the client
         certificates of an API connector)
    """

    def __init__(self, order=None):
        """
        :param list[str] order:
        """
        self.order = StringCollection(order)
