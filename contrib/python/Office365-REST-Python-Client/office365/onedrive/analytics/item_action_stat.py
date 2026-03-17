from office365.runtime.client_value import ClientValue


class ItemActionStat(ClientValue):
    """The itemActionStat resource provides aggregate details about an action over a period of time."""

    def __init__(self, action_count=None, actor_count=None):
        """
        :param int action_count: The number of times the action took place. Read-only.
        :param int actor_count: The number of distinct actors that performed the action. Read-only.
        """
        self.actionCount = action_count
        self.actorCount = actor_count
