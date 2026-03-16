from typing import Optional

from office365.directory.permissions.identity_set import IdentitySet
from office365.onenote.entity_schema_object_model import OnenoteEntitySchemaObjectModel


class OnenoteEntityHierarchyModel(OnenoteEntitySchemaObjectModel):
    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The name of the section."""
        return self.properties.get("displayName", None)

    @property
    def created_by(self):
        # type: () -> IdentitySet
        """Identity of the user, device, and application which created the item. Read-only."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def last_modified_by(self):
        """Identity of the user, device, and application which created the item. Read-only."""
        return self.properties.get("lastModifiedBy", IdentitySet())

    @property
    def last_modified_datetime(self):
        """Gets date and time the item was last modified."""
        return self.properties.get("lastModifiedDateTime", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
                "lastModifiedBy": self.last_modified_by,
            }
            default_value = property_mapping.get(name, None)
        return super(OnenoteEntityHierarchyModel, self).get_property(
            name, default_value
        )
