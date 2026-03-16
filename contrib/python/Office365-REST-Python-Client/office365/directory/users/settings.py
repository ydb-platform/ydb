from typing import Optional

from office365.directory.users.insights_settings import UserInsightsSettings
from office365.directory.users.storage import UserStorage
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.schedule.shifts.preferences import ShiftPreferences


class UserSettings(Entity):
    """The current user settings for content discovery."""

    @property
    def contribution_to_content_discovery_as_organization_disabled(self):
        # type: () -> Optional[bool]
        """Reflects the organization level setting controlling delegate access to the trending API.
        When set to true, the organization doesn't have access to Office Delve. The relevancy of the content
        displayed in Microsoft 365, for example in Suggested sites in SharePoint Home and the Discover view in
        OneDrive for work or school is affected for the whole organization. This setting is read-only and can only
        be changed by administrators in the SharePoint admin center."""
        return self.properties.get(
            "contributionToContentDiscoveryAsOrganizationDisabled", None
        )

    @property
    def contribution_to_content_discovery_disabled(self):
        # type: () -> Optional[bool]
        """When set to true, the delegate access to the user's trending API is disabled.
        When set to true, documents in the user's Office Delve are disabled. When set to true, the relevancy of
        the content displayed in Microsoft 365, for example in Suggested sites in SharePoint Home and the
        Discover view in OneDrive for work or school is affected. Users can control this setting in Office Delve
        """
        return self.properties.get("contributionToContentDiscoveryDisabled", None)

    @property
    def item_insights(self):
        # type: () -> UserInsightsSettings
        """The user's settings for the visibility of meeting hour insights, and insights derived between
        a user and other items in Microsoft 365, such as documents or sites.
        Get userInsightsSettings through this navigation property."""
        return self.properties.get(
            "itemInsights",
            UserInsightsSettings(
                self.context, ResourcePath("itemInsights", self.resource_path)
            ),
        )

    @property
    def shift_preferences(self):
        # type: () -> ShiftPreferences
        return self.properties.get(
            "shiftPreferences",
            ShiftPreferences(
                self.context, ResourcePath("shiftPreferences", self.resource_path)
            ),
        )

    @property
    def storage(self):
        # type: () -> ShiftPreferences
        return self.properties.get(
            "storage",
            UserStorage(self.context, ResourcePath("storage", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "itemInsights": self.item_insights,
                "shiftPreferences": self.shift_preferences,
            }
            default_value = property_mapping.get(name, None)
        return super(UserSettings, self).get_property(name, default_value)
