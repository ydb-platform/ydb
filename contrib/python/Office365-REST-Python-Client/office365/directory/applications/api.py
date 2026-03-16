from office365.directory.permissions.scope import PermissionScope
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection


class ApiApplication(ClientValue):
    """Specifies settings for an application that implements a web API."""

    def __init__(
        self,
        accept_mapped_claims=None,
        known_client_applications=None,
        oauth2_permission_scopes=None,
    ):
        """
        :param str accept_mapped_claims: When true, allows an application to use claims mapping without specifying
            a custom signing key.
        :param list[str] known_client_applications: Used for bundling consent if you have a solution that contains
            two parts: a client app and a custom web API app. If you set the appID of the client app to this value,
            the user only consents once to the client app. Azure AD knows that consenting to the client means
            implicitly consenting to the web API and automatically provisions service principals for both APIs
            at the same time. Both the client and the web API app must be registered in the same tenant.
        :param list[PermissionScope] oauth2_permission_scopes: The definition of the delegated permissions exposed
            by the web API represented by this application registration. These delegated permissions may be requested
            by a client application, and may be granted by users or administrators during consent.
            Delegated permissions are sometimes referred to as OAuth 2.0 scopes.
        """
        self.acceptMappedClaims = accept_mapped_claims
        self.knownClientApplications = StringCollection(known_client_applications)
        self.oauth2PermissionScopes = ClientValueCollection(
            PermissionScope, oauth2_permission_scopes
        )
