from office365.runtime.client_value import ClientValue


class PowerAppsEnvironmentContext(ClientValue):
    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.Online.SharePoint.TenantAdministration.PowerAppsEnvironmentContext"
