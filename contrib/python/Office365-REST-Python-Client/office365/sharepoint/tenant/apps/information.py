from office365.runtime.client_value import ClientValue


class TenantAppInformation(ClientValue):
    """Specifies the information for the tenant-scoped app."""

    def __init__(
        self, app_principal_id=None, app_web_full_url=None, creation_time=None
    ):
        """
        :param str app_principal_id: Specifies the OAuth Id for the tenant-scoped app.
        :param str app_web_full_url: Specifies the web full URL for the tenant-scoped app.
        :param datetime.datetime creation_time: Specifies the creation time for the tenant-scoped app.
        """
        self.AppPrincipalId = app_principal_id
        self.AppWebFullUrl = app_web_full_url
        self.CreationTime = creation_time
