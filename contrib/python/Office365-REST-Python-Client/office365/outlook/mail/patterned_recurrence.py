from office365.outlook.mail.recurrence_pattern import RecurrencePattern
from office365.outlook.mail.recurrence_range import RecurrenceRange
from office365.runtime.client_value import ClientValue


class PatternedRecurrence(ClientValue):
    """
    The recurrence pattern and range. This shared object is used to define the recurrence of the following objects:

        accessReviewScheduleDefinition objects in Azure AD access reviews APIs
        event objects in the calendar API
        unifiedRoleAssignmentScheduleRequest and unifiedRoleEligibilityScheduleRequest objects in PIM
        accessPackageAssignment objects in Azure AD entitlement management.
    """

    def __init__(self, pattern=RecurrencePattern(), recurrence_range=RecurrenceRange()):
        """
        :param RecurrencePattern pattern: The frequency of an event.
             For access reviews:
                 - Do not specify this property for a one-time access review.
                 - Only interval, dayOfMonth, and type (weekly, absoluteMonthly) properties of recurrencePattern
                   are supported.
        :param RecurrenceRange recurrence_range: The duration of an event.
        """
        self.pattern = pattern
        self.range = recurrence_range
