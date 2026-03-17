from typing import Optional

from office365.runtime.types.collections import StringCollection
from office365.teams.schedule.change_tracked_entity import ChangeTrackedEntity


class SchedulingGroup(ChangeTrackedEntity):
    """A logical grouping of users in a shifts (usually by role)."""

    @property
    def is_active(self):
        # type: () -> Optional[bool]
        """Indicates whether the schedulingGroup can be used when creating new entities or updating existing ones"""
        return self.properties.get("isActive", None)

    @property
    def user_ids(self):
        """The list of user IDs that are a member of the schedulingGroup."""
        return self.properties.get("userIds", StringCollection())
