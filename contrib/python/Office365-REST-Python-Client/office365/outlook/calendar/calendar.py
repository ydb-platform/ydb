from typing import Optional

from office365.directory.extensions.extended_property import (
    MultiValueLegacyExtendedProperty,
    SingleValueLegacyExtendedProperty,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.outlook.calendar.email_address import EmailAddress
from office365.outlook.calendar.events.collection import EventCollection
from office365.outlook.calendar.permissions.collection import (
    CalendarPermissionCollection,
)
from office365.outlook.calendar.schedule.information import ScheduleInformation
from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection


class Calendar(Entity):
    """
    A calendar which is a container for events. It can be a calendar for a user, or the default calendar
        of a Microsoft 365 group.
    """

    def __repr__(self):
        return self.name or self.id or self.entity_type_name

    def allowed_calendar_sharing_roles(self, user):
        """
        :param str user: User identifier or principal name
        """
        params = {"user": user}
        return_type = ClientResult(self.context, StringCollection())
        qry = FunctionQuery(self, "allowedCalendarSharingRoles", params, return_type)
        self.context.add_query(qry)
        return return_type

    def get_schedule(
        self, schedules, start_time, end_time, availability_view_interval=30
    ):
        """
        Get the free/busy availability information for a collection of users, distributions lists, or resources
        (rooms or equipment) for a specified time period.

        :param datetime.datetime end_time: The date, time, and time zone that the period ends.
        :param int availability_view_interval: Represents the duration of a time slot in an availabilityView
             in the response. The default is 30 minutes, minimum is 5, maximum is 1440. Optional.
        :param datetime.datetime start_time: The date, time, and time zone that the period starts.
        :param list[str] schedules: A collection of SMTP addresses of users, distribution lists,
            or resources to get availability information for.
        """
        payload = {
            "schedules": schedules,
            "startTime": DateTimeTimeZone.parse(start_time),
            "endTime": DateTimeTimeZone.parse(end_time),
            "availabilityViewInterval": availability_view_interval,
        }
        return_type = ClientResult(
            self.context, ClientValueCollection(ScheduleInformation)
        )
        qry = ServiceOperationQuery(
            self, "getSchedule", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def allowed_online_meeting_providers(self):
        """
        Represent the online meeting service providers that can be used to create online meetings in this calendar.
        Possible values are: unknown, skypeForBusiness, skypeForConsumer, teamsForBusiness.
        """
        return self.properties.get("allowedOnlineMeetingProviders", StringCollection())

    @property
    def can_edit(self):
        # type: () -> Optional[bool]
        """
        true if the user can write to the calendar, false otherwise.
        This property is true for the user who created the calendar.
        This property is also true for a user who has been shared a calendar and granted write access.
        """
        return self.properties.get("canEdit", None)

    @property
    def can_share(self):
        # type: () -> Optional[bool]
        """
        true if the user has the permission to share the calendar, false otherwise.
        Only the user who created the calendar can share it.
        """
        return self.properties.get("canShare", None)

    @property
    def can_view_private_items(self):
        # type: () -> Optional[bool]
        """
        true if the user can read calendar items that have been marked private, false otherwise.
        """
        return self.properties.get("canViewPrivateItems", None)

    @property
    def change_key(self):
        # type: () -> Optional[str]
        """
        Identifies the version of the calendar object. Every time the calendar is changed, changeKey changes as well.
        This allows Exchange to apply changes to the correct version of the object.
        """
        return self.properties.get("changeKey", None)

    @property
    def color(self):
        # type: () -> Optional[str]
        """
        Specifies the color theme to distinguish the calendar from other calendars in a UI.
        The property values are: auto, lightBlue, lightGreen, lightOrange, lightGray, lightYellow, lightTeal,
        lightPink, lightBrown, lightRed, maxColor.
        """
        return self.properties.get("color", None)

    @property
    def default_online_meeting_provider(self):
        # type: () -> Optional[str]
        """
        The default online meeting provider for meetings sent from this calendar.
        Possible values are: unknown, skypeForBusiness, skypeForConsumer, teamsForBusiness.
        """
        return self.properties.get("defaultOnlineMeetingProvider", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """The calendar name"""
        return self.properties.get("name", None)

    @property
    def is_default_calendar(self):
        # type: () -> Optional[bool]
        """
        true if this is the default calendar where new events are created by default, false otherwise.
        """
        return self.properties.get("isDefaultCalendar", None)

    @property
    def is_removable(self):
        # type: () -> Optional[bool]
        """
        Indicates whether this user calendar can be deleted from the user mailbox.
        """
        return self.properties.get("isRemovable", None)

    @property
    def is_tallying_responses(self):
        # type: () -> Optional[bool]
        """
        Indicates whether this user calendar supports tracking of meeting responses.
        Only meeting invites sent from users' primary calendars support tracking of meeting responses.
        """
        return self.properties.get("isTallyingResponses", None)

    @property
    def owner(self):
        """If set, this represents the user who created or added the calendar.
        For a calendar that the user created or added, the owner property is set to the user. For a calendar shared
        with the user, the owner property is set to the person who shared that calendar with the user.
        """
        return self.properties.get("owner", EmailAddress())

    @property
    def events(self):
        # type: () -> EventCollection
        """The events in the calendar. Navigation property. Read-only."""
        return self.properties.get(
            "events",
            EventCollection(self.context, ResourcePath("events", self.resource_path)),
        )

    @property
    def calendar_view(self):
        # type: () -> EventCollection
        """The calendar view for the calendar. Navigation property. Read-only."""
        return self.properties.get(
            "calendarView",
            EventCollection(
                self.context, ResourcePath("calendarView", self.resource_path)
            ),
        )

    @property
    def calendar_permissions(self):
        """The permissions of the users with whom the calendar is shared."""
        return self.properties.get(
            "calendarPermissions",
            CalendarPermissionCollection(
                self.context, ResourcePath("calendarPermissions", self.resource_path)
            ),
        )

    @property
    def multi_value_extended_properties(self):
        # type: () -> EntityCollection[MultiValueLegacyExtendedProperty]
        """The collection of multi-value extended properties defined for the Calendar."""
        return self.properties.get(
            "multiValueExtendedProperties",
            EntityCollection(
                self.context,
                MultiValueLegacyExtendedProperty,
                ResourcePath("multiValueExtendedProperties", self.resource_path),
            ),
        )

    @property
    def single_value_extended_properties(self):
        # type: () -> EntityCollection[SingleValueLegacyExtendedProperty]
        """The collection of single-value extended properties defined for the calendar. Read-only. Nullable."""
        return self.properties.get(
            "singleValueExtendedProperties",
            EntityCollection(
                self.context,
                SingleValueLegacyExtendedProperty,
                ResourcePath("singleValueExtendedProperties", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "allowedOnlineMeetingProviders": self.allowed_online_meeting_providers,
                "calendarView": self.calendar_view,
                "calendarPermissions": self.calendar_permissions,
                "multiValueExtendedProperties": self.multi_value_extended_properties,
                "singleValueExtendedProperties": self.single_value_extended_properties,
            }
            default_value = property_mapping.get(name, None)
        return super(Calendar, self).get_property(name, default_value)
