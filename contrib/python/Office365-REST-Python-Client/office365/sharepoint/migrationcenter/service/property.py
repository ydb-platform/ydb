from office365.runtime.client_value import ClientValue


class MigrationProperty(ClientValue):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationProperty"
