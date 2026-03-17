from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.directory.group import Group
from office365.sharepoint.entity import Entity


class GroupAndUserStatus(Entity):
    @property
    def group(self):
        """Get a Group"""
        return self.properties.get(
            "Group", Group(self.context, ResourcePath("Group", self.resource_path))
        )

    @property
    def entity_type_name(self):
        return "SP.Directory.GroupAndUserStatus"
