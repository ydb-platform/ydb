from office365.directory.identitygovernance.accessreview.stages import (
    AccessReviewStageCollection,
)
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class AccessReviewInstance(Entity):
    """
    Represents a Microsoft Entra access review recurrence. If the parent accessReviewScheduleDefinition is a
    recurring access review, instances represent each recurrence. A review that does not recur will have exactly
    one instance. Instances also represent each unique group being reviewed in the schedule definition.
    If a schedule definition reviews multiple groups, each group will have a unique instance for each recurrence.

    Every accessReviewInstance contains a list of decisions that reviewers can take action on.
    There is one decision per identity being reviewed.
    """

    @property
    def stages(self):
        # type: () -> AccessReviewStageCollection
        """If the instance has multiple stages, this returns the collection of stages.
        A new stage will only be created when the previous stage ends"""
        return self.properties.get(
            "stages",
            AccessReviewStageCollection(
                self.context, ResourcePath("stages", self.resource_path)
            ),
        )
