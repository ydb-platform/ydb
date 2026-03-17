from office365.planner.tasks.check_list_item import PlannerChecklistItem
from office365.runtime.client_value_collection import ClientValueCollection


class PlannerChecklistItems(ClientValueCollection):
    """The plannerChecklistItemCollection resource represents the collection of checklist items on a task.
    It is an Open Type. It is part of the task details object.
    The value in the property-value pair is the checklistItem object.
    """

    def __init__(self, initial_values=None):
        super(PlannerChecklistItems, self).__init__(
            PlannerChecklistItem, initial_values
        )
