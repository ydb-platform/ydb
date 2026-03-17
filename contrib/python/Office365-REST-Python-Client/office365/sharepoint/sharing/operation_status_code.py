class SharingOperationStatusCode:
    """Contains values representing the overall result of a sharing operation."""

    CompletedSuccessfully = 0
    """Indicates the share operation completed without errors."""

    AccessRequestsQueued = 1
    """Indicates the share operation completed and generated requests for access."""

    NoResolvedUsers = -1
    """Indicates the share operation failed as there were no resolved users."""

    AccessDenied = -2
    """Indicates the share operation failed due to insufficient permissions."""

    CrossSiteRequestNotSupported = -3
    """Indicates the share operation failed when attempting a cross-site share, which is not supported."""

    UnknownError = -4
    """Indicates the sharing operation failed due to an unknown error."""

    EmailBodyTooLong = -5
    """The email body text is too long."""

    ListUniqueScopesExceeded = -6
    """The maximum number of unique security scopes in the list has been exceeded."""

    CapabilityDisabled = -7
    """The share operation failed because a sharing capability is disabled in the site."""

    ObjectNotSupported = -8
    """The specified object for the share operation is not supported."""

    NestedGroupsNotSupported = -9
    """A SharePoint group cannot contain another SharePoint group."""

    QuotaExceeded = -10
    """A SharePoint Quota limit exceeded. (eg. group, site, file storage, etc)."""

    InvalidValue = -11
    """A SharePoint invalid value."""

    UserDoesNotExist = -12
    """User does not exist."""

    TooManyChildItemsWithUniqueScopes = -13
    """The share operation failed because the target object has too many child list items with unique security scopes"""
