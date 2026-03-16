from office365.runtime.client_value import ClientValue


class PlannerChecklistItem(ClientValue):
    """
    The plannerChecklistItem resource represents an item in the checklist of a task.
    The checklist on a task is represented by the checklistItems object.
    """

    def __init__(self, title=None):
        """
        :param str|None title: The title of the checklist.
        """
        self.title = title
