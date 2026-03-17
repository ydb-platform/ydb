class SharingCapabilities:
    """External sharing settings on a SharePoint site collection in Office 365"""

    def __init__(self):
        pass

    Disabled = 0
    """External user sharing (share by email) and guest link sharing are both disabled."""

    ExternalUserSharingOnly = 1
    """External user sharing (share by email) is enabled, but guest link sharing is disabled."""

    ExternalUserAndGuestSharing = 2
    """External user sharing (share by email) and guest link sharing are both enabled."""

    ExistingExternalUserSharingOnly = 3
    """Only guests already in your organization's directory."""
