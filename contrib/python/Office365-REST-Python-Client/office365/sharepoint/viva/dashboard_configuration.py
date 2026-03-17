from office365.runtime.client_value import ClientValue


class DashboardConfiguration(ClientValue):

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.EmployeeEngagement.DashboardConfiguration"
