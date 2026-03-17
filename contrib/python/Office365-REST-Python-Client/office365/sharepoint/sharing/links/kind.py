class SharingLinkKind:
    def __init__(self):
        """Specifies the kind of tokenized sharing link"""
        pass

    Uninitialized = 0
    """Indicates the kind is indeterminate or that the value has not been initialized"""

    Direct = 1
    """Indicates a direct link or canonical URL to an object"""

    OrganizationView = 2
    """Indicates an organization access link with view permissions to an object"""

    OrganizationEdit = 3
    """Indicates an organization access link with edit permissions to an object"""

    AnonymousView = 4
    """Indicates an anonymous access link with view permissions to an object"""

    AnonymousEdit = 5
    """Indicates an anonymous access link with edit permissions to an object"""

    Flexible = 6
    """Indicates a tokenized sharing link where properties can change without affecting link URL"""
