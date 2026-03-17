from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.schedule.groups.group import SchedulingGroup
from office365.teams.schedule.shifts.open.change_request import OpenShiftChangeRequest
from office365.teams.schedule.shifts.shift import Shift
from office365.teams.schedule.time_off_reason import TimeOffReason


class Schedule(Entity):
    """A collection of schedulingGroup objects, shift objects, timeOffReason objects,
    and timeOff objects within a team."""

    @property
    def time_zone(self):
        """Indicates the time zone of the shifts team using tz database format. Required."""
        return self.properties.get("timeZone", None)

    @time_zone.setter
    def time_zone(self, value):
        self.set_property("timeZone", value)

    @property
    def open_shift_change_requests(self):
        # type: () -> EntityCollection[OpenShiftChangeRequest]
        """The shifts in the shifts."""
        return self.properties.get(
            "openShiftChangeRequests",
            EntityCollection(
                self.context,
                OpenShiftChangeRequest,
                ResourcePath("openShiftChangeRequests", self.resource_path),
            ),
        )

    @property
    def shifts(self):
        # type: () -> EntityCollection[Shift]
        """The shifts in the shifts."""
        return self.properties.get(
            "shifts",
            EntityCollection(
                self.context, Shift, ResourcePath("shifts", self.resource_path)
            ),
        )

    @property
    def scheduling_groups(self):
        # type: () -> EntityCollection[SchedulingGroup]
        """The logical grouping of users in the shifts (usually by role)."""
        return self.properties.get(
            "schedulingGroups",
            EntityCollection(
                self.context,
                SchedulingGroup,
                ResourcePath("schedulingGroups", self.resource_path),
            ),
        )

    @property
    def time_off_reasons(self):
        # type: () -> EntityCollection[TimeOffReason]
        """The set of reasons for a time off in the schedule."""
        return self.properties.get(
            "timeOffReasons",
            EntityCollection(
                self.context,
                TimeOffReason,
                ResourcePath("timeOffReasons", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "openShiftChangeRequests": self.open_shift_change_requests,
                "schedulingGroups": self.scheduling_groups,
                "timeOffReasons": self.time_off_reasons,
            }
            default_value = property_mapping.get(name, None)
        return super(Schedule, self).get_property(name, default_value)
