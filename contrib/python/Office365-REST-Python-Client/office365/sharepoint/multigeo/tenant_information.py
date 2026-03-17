from office365.sharepoint.entity import Entity


class TenantInformation(Entity):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MultiGeo.Service.TenantInformation"
