from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.picker_settings import PickerSettings


class SharePointSharingSettings(Entity):
    """This class contains the SharePoint UI-specific sharing settings."""

    @property
    def picker_properties(self):
        """An object containing the necessary information to initialize a client people picker control used
        to search for and resolve desired users and groups."""
        return self.properties.get(
            "PickerProperties",
            PickerSettings(
                self.context, ResourcePath("PickerProperties", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "PickerProperties": self.picker_properties,
            }
            default_value = property_mapping.get(name, None)
        return super(SharePointSharingSettings, self).get_property(name, default_value)
