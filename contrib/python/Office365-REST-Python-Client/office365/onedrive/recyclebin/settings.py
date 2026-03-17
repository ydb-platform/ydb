from office365.runtime.client_value import ClientValue


class RecycleBinSettings(ClientValue):
    """Represents settings for the recycleBin resource type."""

    def __init__(self, retention_period_override_days=None):
        """
        :param int retention_period_override_days: Recycle bin retention period override in days for deleted content.
        The default value is 93; the value range is 7 to 180. The setting applies to newly deleted content only.
        Setting this property to null reverts to its default value.
        """
        self.retentionPeriodOverrideDays = retention_period_override_days
