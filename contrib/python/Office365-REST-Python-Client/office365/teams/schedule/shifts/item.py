from office365.runtime.client_value_collection import ClientValueCollection
from office365.teams.schedule.entity import ScheduleEntity
from office365.teams.schedule.shifts.activity import ShiftActivity


class ShiftItem(ScheduleEntity):
    """Represents a version of a shift."""

    def __init__(self, display_name=None, activities=None):
        """
        :param str display_name: The shift label of the shiftItem.
        :param list[ShiftActivity] activities: An incremental part of a shift which can cover details of when and
            where an employee is during their shift. For example, an assignment or a scheduled break or lunch.
        """
        super(ShiftItem, self).__init__()
        self.displayName = display_name
        self.activities = ClientValueCollection(ShiftActivity, activities)
