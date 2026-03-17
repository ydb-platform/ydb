from office365.sharepoint.entity import Entity


class FileStatus(Entity):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.FilePublish.Model.FileStatus"
