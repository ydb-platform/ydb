from office365.runtime.client_value import ClientValue


class SPOAppBillingProperties(ClientValue):
    """ """

    def __init__(self, application_id=None, azure_region=None, is_activated=None):
        """
        :param str application_id: The application ID.
        :param str azure_region: The Azure region.
        :param bool is_activated:
        """
        self.ApplicationId = application_id
        self.AzureRegion = azure_region
        self.IsActivated = is_activated

    @property
    def entity_type_name(self):
        return (
            "Microsoft.Online.SharePoint.TenantAdministration.SPOAppBillingProperties"
        )
