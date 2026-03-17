from typing import Optional

from office365.entity import Entity


class OAuth2PermissionGrant(Entity):
    """
    Represents the delegated permissions that have been granted to an application's service principal.

    Delegated permissions grants can be created as a result of a user consenting the an application's request
    to access an API, or created directly.

    Delegated permissions are sometimes referred to as "OAuth 2.0 scopes" or "scopes".
    """

    @property
    def client_id(self):
        # type: () -> Optional[str]
        """
        The id of the client service principal for the application which is authorized to act on behalf of a signed-in
        user when accessing an API. Required. Supports $filter (eq only).
        """
        return self.properties.get("clientId", None)

    @property
    def consent_type(self):
        # type: () -> Optional[str]
        """
        Indicates if authorization is granted for the client application to impersonate all users or only a
        specific user. AllPrincipals indicates authorization to impersonate all users.
        Principal indicates authorization to impersonate a specific user. Consent on behalf of all users can be granted
        by an administrator. Non-admin users may be authorized to consent on behalf of themselves in some cases,
        for some delegated permissions. Required. Supports $filter (eq only).
        """
        return self.properties.get("consentType", None)

    @property
    def principal_id(self):
        # type: () -> Optional[str]
        """
        The id of the user on behalf of whom the client is authorized to access the resource, when consentType is
        Principal. If consentType is AllPrincipals this value is null. Required when consentType is Principal.
        Supports $filter (eq only).
        """
        return self.properties.get("principalId", None)

    @property
    def resource_id(self):
        # type: () -> Optional[str]
        """
        The id of the resource service principal to which access is authorized. This identifies the API which the
        client is authorized to attempt to call on behalf of a signed-in user. Supports $filter (eq only).
        """
        return self.properties.get("resourceId", None)

    @property
    def scope(self):
        # type: () -> Optional[str]
        """
        A space-separated list of the claim values for delegated permissions which should be included in access tokens
        for the resource application (the API). For example, openid User.Read GroupMember.Read.All. Each claim value
        should match the value field of one of the delegated permissions defined by the API, listed in the
        oauth2PermissionScopes property of the resource service principal. Must not exceed 3850 characters in length.
        """
        return self.properties.get("scope", None)
