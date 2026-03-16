from typing import Optional

from office365.entity import Entity
from office365.outlook.calendar.email_address import EmailAddress
from office365.runtime.types.collections import StringCollection


class CalendarPermission(Entity):
    """
    The permissions of a user with whom the calendar has been shared or delegated in an Outlook client.

    Get, update, and delete of calendar permissions is supported on behalf of only the calendar owner.

    Getting the calendar permissions of a calendar on behalf of a sharee or delegate returns
    an empty calendar permissions collection.

    Once a sharee or delegate has been set up for a calendar, you can update only the role property to change
    the permissions of a sharee or delegate. You cannot update the allowedRoles, emailAddress, isInsideOrganization,
    or isRemovable property. To change these properties, you should delete the corresponding calendarPermission
    object and create another sharee or delegate in an Outlook client.
    """

    @property
    def allowed_roles(self):
        """
        List of allowed sharing or delegating permission levels for the calendar.
        Possible values are: none, freeBusyRead, limitedRead, read, write, delegateWithoutPrivateEventAccess,
        delegateWithPrivateEventAccess, custom.
        """
        return self.properties.get("allowedRoles", StringCollection())

    @property
    def email_address(self):
        """
        Represents a sharee or delegate who has access to the calendar.
        For the "My Organization" sharee, the address property is null. Read-only.
        """
        return self.properties.get("emailAddress", EmailAddress())

    @property
    def is_inside_organization(self):
        # type: () -> Optional[bool]
        """
        True if the user in context (sharee or delegate) is inside the same organization as the calendar owner
        """
        return self.properties.get("isInsideOrganization", None)

    @property
    def is_removable(self):
        # type: () -> Optional[bool]
        """
        True if the user can be removed from the list of sharees or delegates for the specified calendar,
        false otherwise. The "My organization" user determines the permissions other people within your organization
        have to the given calendar. You cannot remove "My organization" as a sharee to a calendar.
        """
        return self.properties.get("isRemovable", None)

    @property
    def role(self):
        # type: () -> Optional[str]
        """Current permission level of the calendar sharee or delegate."""
        return self.properties.get("role", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "allowedRoles": self.allowed_roles,
                "emailAddress": self.email_address,
            }
            default_value = property_mapping.get(name, None)
        return super(CalendarPermission, self).get_property(name, default_value)
