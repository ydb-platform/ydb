from office365.directory.extensions.extended_property import (
    MultiValueLegacyExtendedProperty,
    SingleValueLegacyExtendedProperty,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class ContactFolder(Entity):
    """A folder that contains contacts."""

    @property
    def contacts(self):
        """The contacts in the folder. Navigation property. Read-only. Nullable."""
        from office365.outlook.contacts.contact import Contact

        return self.properties.get(
            "contacts",
            EntityCollection(
                self.context, Contact, ResourcePath("contacts", self.resource_path)
            ),
        )

    @property
    def child_folders(self):
        # type: () -> EntityCollection["ContactFolder"]
        """The collection of child folders in the folder. Navigation property. Read-only. Nullable."""
        return self.properties.get(
            "childFolders",
            EntityCollection(
                self.context,
                ContactFolder,
                ResourcePath("childFolders", self.resource_path),
            ),
        )

    @property
    def multi_value_extended_properties(self):
        # type: () -> EntityCollection[MultiValueLegacyExtendedProperty]
        """The collection of multi-value extended properties defined for the Contact folder."""
        return self.properties.get(
            "multiValueExtendedProperties",
            EntityCollection(
                self.context,
                MultiValueLegacyExtendedProperty,
                ResourcePath("multiValueExtendedProperties", self.resource_path),
            ),
        )

    @property
    def single_value_extended_properties(self):
        # type: () -> EntityCollection[SingleValueLegacyExtendedProperty]
        """The collection of single-value extended properties defined for the Contact folder."""
        return self.properties.get(
            "singleValueExtendedProperties",
            EntityCollection(
                self.context,
                SingleValueLegacyExtendedProperty,
                ResourcePath("singleValueExtendedProperties", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "childFolders": self.child_folders,
                "multiValueExtendedProperties": self.multi_value_extended_properties,
                "singleValueExtendedProperties": self.single_value_extended_properties,
            }
            default_value = property_mapping.get(name, None)
        return super(ContactFolder, self).get_property(name, default_value)
