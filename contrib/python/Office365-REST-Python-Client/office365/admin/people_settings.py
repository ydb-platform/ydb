from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.people.profile_card_property import ProfileCardProperty
from office365.runtime.paths.resource_path import ResourcePath


class PeopleAdminSettings(Entity):
    """Represents a setting to control people-related admin settings in the tenant."""

    @property
    def profile_card_properties(self):
        """Contains a collection of the properties an administrator has defined as visible on the
        Microsoft 365 profile card."""
        return self.properties.get(
            "profileCardProperties",
            EntityCollection(
                self.context,
                ProfileCardProperty,
                ResourcePath("profileCardProperties", self.resource_path),
            ),
        )
