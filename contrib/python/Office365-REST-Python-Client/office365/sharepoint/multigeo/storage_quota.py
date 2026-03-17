from office365.sharepoint.entity import Entity


class StorageQuota(Entity):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MultiGeo.Service.StorageQuota"
