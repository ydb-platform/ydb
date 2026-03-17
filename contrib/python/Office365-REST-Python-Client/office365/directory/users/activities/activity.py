from datetime import datetime
from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class UserActivity(Entity):
    """
    Represents a single activity within an app - for example, a TV show, a document, or a current campaign
    in a video game. When a user engages with that activity, the engagement is captured as a history item
    that indicates the start and end time for that activity. As the user re-engages with that activity over time,
    multiple history items are recorded for a single user activity.
    """

    @property
    def activation_url(self):
        # type: () -> Optional[str]
        """URL used to launch the activity in the best native experience represented by the appId.
        Might launch a web-based app if no native app exists
        """
        return self.properties.get("activationUrl", None)

    @property
    def activity_source_host(self):
        # type: () -> Optional[str]
        """Required. URL for the domain representing the cross-platform identity mapping for the app.
        Mapping is stored either as a JSON file hosted on the domain or configurable via Windows Dev Center.
        The JSON file is named cross-platform-app-identifiers and is hosted at root of your HTTPS domain,
        either at the top level domain or include a sub domain.
        For example: https://contoso.com or https://myapp.contoso.com but NOT https://myapp.contoso.com/somepath.
        You must have a unique file and domain (or sub domain) per cross-platform app identity.
        For example, a separate file and domain is needed for Word vs. PowerPoint.
        """
        return self.properties.get("activitySourceHost", None)

    @property
    def app_activity_id(self):
        # type: () -> Optional[str]
        """
        The unique activity ID in the context of the app - supplied by caller and immutable thereafter.
        """
        return self.properties.get("appActivityId", None)

    @property
    def app_display_name(self):
        # type: () -> Optional[str]
        """
        Short text description of the app used to generate the activity for use in cases when the app is
        not installed on the user’s local device.
        """
        return self.properties.get("appDisplayName", None)

    @property
    def created_datetime(self):
        """Set by the server. DateTime in UTC when the object was created on the server."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def expiration_datetime(self):
        """Set by the server. DateTime in UTC when the object was created on the server."""
        return self.properties.get("expirationDateTime", datetime.min)

    @property
    def history_items(self):
        """NavigationProperty/Containment; navigation property to the associated activity."""
        from office365.directory.users.activities.history_item import (
            ActivityHistoryItem,
        )

        return self.properties.get(
            "historyItems",
            EntityCollection(
                self.context,
                ActivityHistoryItem,
                ResourcePath("historyItems", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdDateTime": self.created_datetime,
                "expirationDateTime": self.expiration_datetime,
                "historyItems": self.history_items,
            }
            default_value = property_mapping.get(name, None)
        return super(UserActivity, self).get_property(name, default_value)
