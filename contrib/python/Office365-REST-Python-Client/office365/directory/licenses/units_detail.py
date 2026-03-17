from office365.runtime.client_value import ClientValue


class LicenseUnitsDetail(ClientValue):
    """"""

    def __init__(self, enabled=None, locked_out=None, suspended=None, warning=None):
        """
        :param int enabled: The number of units that are enabled for the active subscription of the service SKU.
        """
        self.enabled = enabled
        self.lockedOut = locked_out
        self.suspended = suspended
        self.warning = warning
