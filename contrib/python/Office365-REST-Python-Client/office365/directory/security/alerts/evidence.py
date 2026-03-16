from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class AlertEvidence(ClientValue):
    """The alertEvidence base type and its derived evidence types provide a means to organize and track rich data
    about each artifact involved in an alert."""

    def __init__(self, created_datetime=None, detailed_roles=None):
        """
        :param datetime.datetime created_datetime: The date and time when the evidence was created and added to
             the alert.
        :param list[str] detailed_roles: Detailed description of the entity role/s in an alert. Values are free-form.
        """
        self.createdDateTime = created_datetime
        self.detailedRoles = StringCollection(detailed_roles)
