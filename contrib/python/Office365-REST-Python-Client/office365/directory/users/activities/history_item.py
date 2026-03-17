import datetime
from typing import Optional

from office365.directory.users.activities.activity import UserActivity
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class ActivityHistoryItem(Entity):
    """
    Represents a history item for an activity in an app. User activities represent a single destination within
    your app; for example, a TV show, a document, or a current campaign in a video game.
    When a user engages with that activity, the engagement is captured as a history item that indicates
    the start and end time for that activity. As the user re-engages with that activity over time, multiple
    history items are recorded for a single user activity.
    """

    @property
    def active_duration_seconds(self):
        # type: () -> Optional[int]
        """
        The duration of active user engagement. if not supplied, this is calculated from the startedDateTime
        and lastActiveDateTime.
        """
        return self.properties.get("activeDurationSeconds", None)

    @property
    def created_datetime(self):
        """Set by the server. DateTime in UTC when the object was created on the server."""
        return self.properties.get("createdDateTime", datetime.datetime.min)

    @property
    def activity(self):
        """NavigationProperty/Containment; navigation property to the associated activity."""
        return self.properties.get(
            "activity",
            UserActivity(self.context, ResourcePath("activity", self.resource_path)),
        )
