from office365.runtime.client_value import ClientValue


class SharingAbilityStatus(ClientValue):
    """Represents the status for a specific sharing capability for the current user."""

    def __init__(self, disabled_reason=None, enabled=None):
        """
        :param str disabled_reason:  Indicates the reason why the capability is disabled if the capability is disabled
            for any reason.
        :param bool enabled: Indicates whether capability is enabled.
        """
        self.disabledReason = disabled_reason
        self.enabled = enabled

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharingAbilityStatus"
