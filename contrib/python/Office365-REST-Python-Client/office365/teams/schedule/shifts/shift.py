from office365.teams.schedule.change_tracked_entity import ChangeTrackedEntity
from office365.teams.schedule.shifts.item import ShiftItem


class Shift(ChangeTrackedEntity):
    """
    Represents a unit of scheduled work in a shifts.
    """

    @property
    def draft_shift(self):
        """
        The draft version of this shift that is viewable by managers.
        """
        return self.properties.get("draftShift", ShiftItem())

    @property
    def shared_shift(self):
        """
        The shared version of this shift that is viewable by both employees and managers.
        """
        return self.properties.get("sharedShift", ShiftItem())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "draftShift": self.draft_shift,
                "sharedShift": self.shared_shift,
            }
            default_value = property_mapping.get(name, None)
        return super(Shift, self).get_property(name, default_value)
