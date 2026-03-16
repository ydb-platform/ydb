from office365.runtime.client_value import ClientValue


class AlertCreationInformation(ClientValue):
    """An object that contain the properties used to create a new SP.Alert"""

    def __init__(self, alert_frequency, template_name, alert_type):
        """
        :param int alert_frequency: Gets or sets the time interval for sending the alert.
        :param int alert_type: Gets or sets the alert type.
        """
        super(AlertCreationInformation, self).__init__()
        self.AlertFrequency = alert_frequency
        self.AlertTemplateName = template_name
        self.AlertType = alert_type
