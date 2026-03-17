from office365.runtime.client_value import ClientValue


class HubSitePermission(ClientValue):
    def __init__(self, display_name=None, principal_name=None, rights=None):
        """
        :param str display_name:
        :param str principal_name:
        :param int rights:
        """
        self.DisplayName = display_name
        self.PrincipalName = principal_name
        self.Rights = rights

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.HubSitePermission"
