from office365.entity import Entity
from office365.planner.category_descriptions import PlannerCategoryDescriptions
from office365.planner.user_ids import PlannerUserIds


class PlannerPlanDetails(Entity):
    """
    The plannerPlanDetails resource represents the additional information about a plan.
    Each plan object has a details object.
    """

    @property
    def category_descriptions(self):
        # type: () -> PlannerCategoryDescriptions
        """
        An object that specifies the descriptions of the 25 categories that can be associated with tasks in the plan.
        """
        return self.properties.get(
            "categoryDescriptions", PlannerCategoryDescriptions()
        )

    @property
    def shared_with(self):
        # type: () -> PlannerUserIds
        """
        Set of user IDs that this plan is shared with. If you're using Microsoft 365 groups, use the Groups
        API to manage group membership to share the group's plan. You can also add existing members of the group to
        this collection, although it isn't required for them to access the plan owned by the group.
        """
        return self.properties.get("sharedWith", PlannerUserIds())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "categoryDescriptions": self.category_descriptions,
                "sharedWith": self.shared_with,
            }
            default_value = property_mapping.get(name, None)
        return super(PlannerPlanDetails, self).get_property(name, default_value)
