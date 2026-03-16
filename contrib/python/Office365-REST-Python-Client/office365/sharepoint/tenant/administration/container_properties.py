from office365.runtime.client_value import ClientValue


class SPContainerProperties(ClientValue):
    """"""

    def __init__(self, AllowEditing=None, AuthenticationContextName=None):
        """
        :param bool AllowEditing:
        :param str AuthenticationContextName:
        """
        self.AllowEditing = AllowEditing
        self.AuthenticationContextName = AuthenticationContextName

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SPContainerProperties"
