from office365.runtime.client_value import ClientValue


class DeviceManagementSettings(ClientValue):
    """"""

    def __init__(
        self,
        device_compliance_checkin_threshold_days=None,
        is_scheduled_action_enabled=None,
        secure_by_default=None,
    ):
        """
        :param int device_compliance_checkin_threshold_days: The number of days a device is allowed to go without
            checking in to remain compliant.
        :param bool is_scheduled_action_enabled: Is feature enabled or not for scheduled action for rule.
        :param bool secure_by_default: Device should be noncompliant when there is no compliance policy targeted when
            this is true
        """
        self.deviceComplianceCheckinThresholdDays = (
            device_compliance_checkin_threshold_days
        )
        self.isScheduledActionEnabled = is_scheduled_action_enabled
        self.secureByDefault = secure_by_default
