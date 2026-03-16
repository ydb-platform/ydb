class PrincipalSource:
    """Specifies the source of a principal."""

    def __init__(self):
        pass

    None_ = 0
    """Do not specify a principal source."""

    UserInfoList = 1
    """Use the user information list as the source."""

    Windows = 2
    """Use Windows as the source."""

    MembershipProvider = 4
    """Use the current membership provider as the source."""

    RoleProvider = 8
    """Use the current role provider as the source"""

    All = 15
    """Use all principal sources."""
