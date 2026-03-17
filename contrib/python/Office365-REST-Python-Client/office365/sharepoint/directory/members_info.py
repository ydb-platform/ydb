from office365.sharepoint.entity import Entity


class MembersInfo(Entity):
    @property
    def entity_type_name(self):
        return "SP.Directory.MembersInfo"
