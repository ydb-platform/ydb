from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class CollaborativeUsers(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.TenantAdmin.CollaborativeUsers"


class CollaborationInsightsData(ClientValue):
    def __init__(self, last_report_date=None, collaborative_users=None):
        """
        :param str last_report_date:
        :param list[CollaborativeUsers] collaborative_users:
        """
        self.collaborativeUsers = ClientValueCollection(
            CollaborativeUsers, collaborative_users
        )
        self.lastReportDate = last_report_date

    @property
    def entity_type_name(self):
        return (
            "Microsoft.SharePoint.Administration.TenantAdmin.CollaborationInsightsData"
        )
