class SiteStatus:
    """Status of a modern SharePoint site"""

    def __init__(self):
        pass

    NotFound = 0
    """Not Found. The site doesn't exist."""

    Provisioning = 1
    """Provisioning. The site is currently being provisioned."""

    Ready = 2
    """Ready. The site has been created."""

    Error = 3
    """Error. An error occurred while provisioning the site."""
