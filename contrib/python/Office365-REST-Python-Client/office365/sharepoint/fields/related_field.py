from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class RelatedField(Entity):
    """Represents a Lookup Field that points to a given list on a Web site."""

    @property
    def field_id(self):
        # type: () -> Optional[str]
        """Gets the field id of the corresponding Lookup Field."""
        return self.properties.get("FieldId", None)

    @property
    def list_id(self):
        # type: () -> Optional[str]
        """Gets the ID of the List containing the corresponding Lookup Field."""
        return self.properties.get("ListId", None)

    @property
    def web_id(self):
        # type: () -> Optional[str]
        """Gets the ID of the Web containing the corresponding Lookup Field."""
        return self.properties.get("WebId", None)

    @property
    def relationship_delete_behavior(self):
        # type: () -> Optional[int]
        """Gets delete behavior of the corresponding Lookup Field."""
        return self.properties.get("RelationshipDeleteBehavior", None)

    @property
    def lookup_list(self):
        """Specifies the List that the corresponding Lookup Field looks up to."""
        from office365.sharepoint.lists.list import List

        return self.properties.get(
            "LookupList",
            List(self.context, ResourcePath("LookupList", self.resource_path)),
        )

    def set_property(self, name, value, persist_changes=True):
        super(RelatedField, self).set_property(name, value, persist_changes)
        if name == "FieldId" and self._resource_path is None:
            self._resource_path = self.parent_collection.get_by_field_id(
                value
            ).resource_path
        return self

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"LookupList": self.lookup_list}
            default_value = property_mapping.get(name, None)
        return super(RelatedField, self).get_property(name, default_value)
