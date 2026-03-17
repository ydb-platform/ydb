from office365.sharepoint.entity import Entity


class NonQuotaBackfillApi(Entity):

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.QuotaManagement.Consumer.NonQuotaBackfillApi"
