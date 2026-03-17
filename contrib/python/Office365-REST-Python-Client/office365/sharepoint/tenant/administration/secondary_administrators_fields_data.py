from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class SecondaryAdministratorsFieldsData(ClientValue):
    def __init__(self, site_id=None, emails=None, names=None):
        """
        :type emails: List[str] or None
        :type names: List[str] or None
        :type site_id: str or None
        """
        super(SecondaryAdministratorsFieldsData, self).__init__()
        self.secondaryAdministratorEmails = StringCollection(emails)
        self.secondaryAdministratorLoginNames = StringCollection(names)
        self.siteId = site_id

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SecondaryAdministratorsFieldsData"
