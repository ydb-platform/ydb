from office365.runtime.client_value import ClientValue


class SecondaryAdministratorsInfo(ClientValue):
    def __init__(self, email=None, loginName=None, userPrincipalName=None):
        """

        :param str email:
        :param str loginName:
        :param str userPrincipalName:
        """
        super(SecondaryAdministratorsInfo, self).__init__()
        self.email = email
        self.loginName = loginName
        self.userPrincipalName = userPrincipalName

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.Online.SharePoint.TenantAdministration.SecondaryAdministratorsInfo"
