from office365.runtime.client_value import ClientValue


class AppIdentity(ClientValue):
    """Indicates the identity of the application that performed the action or was changed.
    Includes application ID, name, and service principal ID and name. This resource is used by the
    Get directoryAudit operation."""

    def __init__(
        self,
        app_id=None,
        display_name=None,
        service_principal_id=None,
        service_principal_name=None,
    ):
        """
        :param str app_id: Refers to the Unique GUID representing Application Id in the Azure Active Directory.
        :param str display_name: Refers to the Application Name displayed in the Azure Portal.
        :param str service_principal_id: Refers to the Unique GUID indicating Service Principal Id in Azure Active
            Directory for the corresponding App.
        :param str service_principal_name: Refers to the Service Principal Name is the Application name in the tenant.
        """
        self.appId = app_id
        self.displayName = display_name
        self.servicePrincipalId = service_principal_id
        self.servicePrincipalName = service_principal_name
