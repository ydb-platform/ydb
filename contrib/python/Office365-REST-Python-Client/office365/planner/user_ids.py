from office365.runtime.client_value import ClientValue


class PlannerUserIds(ClientValue):
    """The plannerUserIds resource represents the list of users IDs that a plan is shared with, and is an Open Type.
    If you're using Microsoft 365 groups, use the Groups API to manage group membership to share the group's plan.
    You can also add existing members of the group to this collection though it isn't required for them to access
    the plan owned by the group."""
