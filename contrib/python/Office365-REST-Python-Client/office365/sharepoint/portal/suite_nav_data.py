from office365.sharepoint.entity import Entity


class SuiteNavData(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.SuiteNavData"
