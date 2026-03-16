from office365.runtime.client_value import ClientValue


class AssignedPlan(ClientValue):
    """
    The assignedPlans property of both the user entity and the organization entity is a collection of assignedPlan.
    """

    def __init__(
        self,
        assigned_datetime=None,
        capability_status=None,
        service=None,
        service_plan_id=None,
    ):
        """
        :param datetime.datetime assigned_datetime: The date and time at which the plan was assigned.
        :param str capability_status: Condition of the capability assignment.
            The possible values are Enabled, Warning, Suspended, Deleted, LockedOut.
            See a detailed description of each value.
        :param str service: The name of the service; for example, exchange.
        :param str service_plan_id: A GUID that identifies the service plan. For a complete list of GUIDs and their
            equivalent friendly service names, see Product names and service plan identifiers for licensing.
        """
        self.assignedDateTime = assigned_datetime
        self.capabilityStatus = capability_status
        self.service = service
        self.servicePlanId = service_plan_id

    def __repr__(self):
        return self.service
