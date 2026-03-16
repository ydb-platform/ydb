from office365.runtime.client_value import ClientValue


class PermissionScope(ClientValue):
    """
    Represents the definition of a delegated permission.

    Delegated permissions can be requested by client applications needing an access token to the API which defined the
    permissions. Delegated permissions can be requested dynamically, using the scopes parameter in an authorization
    request to the Microsoft identity platform, or statically, through the requiredResourceAccess collection on the
    application object.
    """

    def __init__(
        self,
        admin_consent_display_name=None,
        admin_consent_description=None,
        id_=None,
        is_enabled=None,
        origin=None,
        type_=None,
        user_consent_description=None,
        user_consent_display_name=None,
        value=None,
    ):
        """
        :param str admin_consent_display_name: The permission's title, intended to be read by an administrator granting
            the permission on behalf of all users.
        :param str admin_consent_description: A description of the delegated permissions, intended to be read
            by an administrator granting the permission on behalf of all users. This text appears in tenant-wide
            admin consent experiences.
        :param str id_: Unique delegated permission identifier inside the collection of delegated permissions defined
            for a resource application.
        :param str is_enabled: When creating or updating a permission, this property must be set to true
            (which is the default). To delete a permission, this property must first be set to false.
            At that point, in a subsequent call, the permission may be removed.

        :param str value: Specifies the value to include in the scp (scope) claim in access tokens.
        """
        self.adminConsentDescription = admin_consent_description
        self.adminConsentDisplayName = admin_consent_display_name
        self.id = id_
        self.isEnabled = is_enabled
        self.origin = origin
        self.type = type_
        self.userConsentDescription = user_consent_description
        self.userConsentDisplayName = user_consent_display_name
        self.value = value
