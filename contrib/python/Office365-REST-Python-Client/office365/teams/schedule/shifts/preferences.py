from office365.runtime.client_value_collection import ClientValueCollection
from office365.teams.schedule.change_tracked_entity import ChangeTrackedEntity
from office365.teams.schedule.shifts.availability import ShiftAvailability


class ShiftPreferences(ChangeTrackedEntity):
    """Represents a user's availability to be assigned shifts in the schedule."""

    @property
    def availability(self):
        """
        Availability of the user to be scheduled for work and its recurrence pattern.
        """
        return self.properties.get(
            "availability", ClientValueCollection(ShiftAvailability)
        )
