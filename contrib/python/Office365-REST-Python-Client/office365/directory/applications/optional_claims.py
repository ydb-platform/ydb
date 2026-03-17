from office365.directory.applications.optional_claim import OptionalClaim
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class OptionalClaims(ClientValue):
    """An application can configure optional claims to be returned in each of three types of tokens
    (ID token, access token, SAML 2 token) it can receive from the security token service.
    An application can configure a different set of optional claims to be returned in each token type.
    The optionalClaims property of the application is an optionalClaims object."""

    def __init__(self, access_token=None, id_token=None, saml2_token=None):
        """ """
        self.accessToken = ClientValueCollection(OptionalClaim, access_token)
        self.idToken = ClientValueCollection(OptionalClaim, id_token)
        self.saml2Token = ClientValueCollection(OptionalClaim, saml2_token)
