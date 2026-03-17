from office365.entity_collection import EntityCollection
from office365.outlook.calendar.email_address import EmailAddress
from office365.outlook.calendar.permissions.permission import CalendarPermission


class CalendarPermissionCollection(EntityCollection[CalendarPermission]):
    def __init__(self, context, resource_path=None):
        super(CalendarPermissionCollection, self).__init__(
            context, CalendarPermission, resource_path
        )

    def add(self, email_address, role):
        """
        Create a calendarPermission resource to specify the identity and role of the user with whom the specified
        calendar is being shared or delegated.
        :param str or EmailAddress email_address: Represents a sharee or delegate who has access to the calendar.
        :param str role: Permission level of the calendar sharee or delegate
        """
        if not isinstance(email_address, EmailAddress):
            email_address = EmailAddress(email_address)
        props = {"emailAddress": email_address, "role": str(role)}
        return super(CalendarPermissionCollection, self).add(**props)
