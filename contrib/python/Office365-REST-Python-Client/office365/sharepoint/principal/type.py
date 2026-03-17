class PrincipalType:
    """Specifies the type of a principal."""

    def __init__(self):
        pass

    None_ = 0
    """Do not specify a principal type."""

    User = 1
    """A user principal type."""

    DistributionList = 2
    """A distribution list principal type."""

    SecurityGroup = 4
    """A security group principal type."""

    SharePointGroup = 8
    """A SharePoint group principal type."""

    All = 15
    """All principal types."""
