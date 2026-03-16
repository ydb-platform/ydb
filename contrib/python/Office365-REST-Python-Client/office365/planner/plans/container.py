from office365.runtime.client_value import ClientValue


class PlannerPlanContainer(ClientValue):
    """
    Represents a container for a plannerPlan. The container is a resource that specifies authorization rules and the
    lifetime of the plan. This means that only the people who are authorized to work with the resource containing
    the plan will be able to work with the plan and the tasks within it. When the containing resource is deleted,
    the contained plans are also deleted. The properties of the plannerPlanContainer cannot be changed after the plan
    is created.
    """

    def __init__(self, container_id=None, type_=None, url=None):
        """
        :param str container_id: The identifier of the resource that contains the plan
        :param str type_: The type of the resource that contains the plan. For supported types, see the previous
            table. Possible values are: group, unknownFutureValue, roster. Use the Prefer: include-unknown-enum-members
            request header to get the following value in this evolvable enum: roster.
        :param str url: 	The full canonical URL of the container.
        """
        self.containerId = container_id
        self.type = type_
        self.url = url
