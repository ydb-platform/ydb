class CalendarRoleType:
    def __init__(self):
        pass

    none = 0
    """Calendar is not shared with the user."""

    freeBusyRead = 1
    """User is a sharee who can view free/busy status of the owner on the calendar."""

    limitedRead = 2
    """User is a sharee who can view free/busy status, and titles and locations of the events on the calendar."""

    read = 3
    """User is a sharee who can view all the details of the events on the calendar, except for the owner's private
    events."""

    write = 4
    """User is a sharee who can view all the details (except for private events) and edit events on the calendar."""

    delegateWithoutPrivateEventAccess = 5
    """User is a delegate who has write access but cannot view information of the owner's private events on the
    calendar."""

    delegateWithPrivateEventAccess = 6
    """User is a delegate who has write access and can view information of the owner's private events on the calendar"""

    custom = 7
    """User has custom permissions to the calendar."""
