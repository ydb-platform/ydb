from office365.runtime.client_value import ClientValue


class ResourceAccess(ClientValue):
    """Object used to specify an OAuth 2.0 permission scope or an app role that an application requires,
    through the resourceAccess property of the requiredResourceAccess resource type."""

    def __init__(self, id_=None, type_=None):
        """
        :param str id_: The unique identifier of an app role or delegated permission exposed by the resource
            application. For delegated permissions, this should match the id property of one of the delegated
            permissions in the oauth2PermissionScopes collection of the resource application's service principal.
            For app roles (application permissions), this should match the id property of an app role in the appRoles
            collection of the resource application's service principal.
        :param str type_: Specifies whether the id property references a delegated permission or an app role
            (application permission). The possible values are: Scope (for delegated permissions) or Role (for app roles).
        """
        self.id = id_
        self.type = type_

    def __repr__(self):
        return "ResourceAccess(id={!r}, type={!r})".format(self.id, self.type)

    @property
    def type_name(self):
        return "Delegated" if self.type == "Scope" else "Application"
