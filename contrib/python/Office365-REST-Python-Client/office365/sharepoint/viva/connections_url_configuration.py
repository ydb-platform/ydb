from office365.runtime.client_value import ClientValue


class VivaConnectionsUrlConfiguration(ClientValue):
    def __init__(self, content_url=None, dashboard_not_configured_warning=None):
        """
        :param str content_url:
        :param str dashboard_not_configured_warning:
        """
        self.ContentUrl = content_url
        self.DashboardNotConfiguredWarning = dashboard_not_configured_warning
