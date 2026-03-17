from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class AppRole(ClientValue):
    """
    Represents an application role that can be requested by (and granted to) a client application,
    or that can be used to assign an application to users or groups in a specified role.
    """

    def __init__(
        self,
        id_=None,
        allowed_member_types=None,
        description=None,
        display_name=None,
        is_enabled=None,
        origin=None,
        value=None,
    ):
        """
        :param list[str] allowed_member_types: Specifies whether this app role can be assigned to users and groups
            (by setting to ["User"]), to other application's (by setting to ["Application"], or both (by setting to
            ["User", "Application"]). App roles supporting assignment to other applications' service principals are
            also known as application permissions. The "Application" value is only supported for app roles defined
            on application entities.
        :param str value:
        """
        self.id = id_
        self.allowedMemberTypes = StringCollection(allowed_member_types)
        self.description = description
        self.displayName = display_name
        self.isEnabled = is_enabled
        self.origin = origin
        self.value = value

    def __str__(self):
        return "{0}  {1}".format(self.allowedMemberTypes, self.value)

    def __repr__(self):
        return self.value or self.id or self.entity_type_name
