from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity


class MembershipResult(Entity):
    @property
    def groups_list(self):
        return self.properties.get("GroupsList", StringCollection())

    @property
    def entity_type_name(self):
        return "SP.Directory.MembershipResult"
