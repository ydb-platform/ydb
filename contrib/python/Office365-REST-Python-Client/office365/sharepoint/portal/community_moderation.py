from office365.sharepoint.entity import Entity


class CommunityModeration(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.CommunityModeration"
