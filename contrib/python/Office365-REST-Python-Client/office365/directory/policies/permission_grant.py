from office365.directory.permissions.grants.condition_set import (
    PermissionGrantConditionSet,
)
from office365.directory.policies.base import PolicyBase
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class PermissionGrantPolicy(PolicyBase):
    """
    A permission grant policy is used to specify the conditions under which consent can be granted.

    A permission grant policy consists of a list of includes condition sets, and a list of excludes condition sets.
    For an event to match a permission grant policy, it must match at least one of the includes conditions sets,
    and none of the excludes condition sets.
    """

    @property
    def excludes(self):
        # type: () -> EntityCollection[PermissionGrantConditionSet]
        """
        Condition sets which are excluded in this permission grant policy.
        This navigation is automatically expanded on GET.
        """
        return self.properties.get(
            "excludes",
            EntityCollection(
                self.context,
                PermissionGrantConditionSet,
                ResourcePath("excludes", self.resource_path),
            ),
        )

    @property
    def includes(self):
        # type: () -> EntityCollection[PermissionGrantConditionSet]
        """
        Condition sets which are included in this permission grant policy.
        This navigation is automatically expanded on GET.
        """
        return self.properties.get(
            "includes",
            EntityCollection(
                self.context,
                PermissionGrantConditionSet,
                ResourcePath("includes", self.resource_path),
            ),
        )
