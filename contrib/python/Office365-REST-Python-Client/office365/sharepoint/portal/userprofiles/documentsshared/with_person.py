from office365.sharepoint.entity import Entity


class DocumentsSharedWithPerson(Entity):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.DocumentsSharedWithPerson"
