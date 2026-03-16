from office365.outlook.mail.location_constraint_item import LocationConstraintItem
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class LocationConstraint(ClientValue):
    """The conditions stated by a client for the location of a meeting."""

    def __init__(self, is_required=None, locations=None, suggest_location=None):
        """
        :param bool is_required: 	The client requests the service to include in the response a meeting location
            for the meeting. If this is true and all the resources are busy, findMeetingTimes will not return any
            meeting time suggestions. If this is false and all the resources are busy, findMeetingTimes would still
            look for meeting times without locations.
        :param list[LocationConstraintItem] locations: Constraint information for one or more locations that the client
            requests for the meeting
        :param bool suggest_location: The client requests the service to suggest one or more meeting locations.
        """
        self.isRequired = is_required
        self.locations = ClientValueCollection(LocationConstraintItem, locations)
        self.suggestLocation = suggest_location
