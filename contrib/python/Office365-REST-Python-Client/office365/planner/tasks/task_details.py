from typing import Optional

from office365.entity import Entity
from office365.planner.external_references import PlannerExternalReferences
from office365.planner.tasks.check_list_items import PlannerChecklistItems


class PlannerTaskDetails(Entity):
    """
    The plannerTaskDetails resource represents the additional information about a task.
    Each task object has a details object.
    """

    @property
    def checklist(self):
        # type: () -> PlannerChecklistItems
        """
        The collection of checklist items on the task.
        """
        return self.properties.get("checklist", PlannerChecklistItems())

    @property
    def description(self):
        # type: () -> Optional[str]
        """Description of the task."""
        return self.properties.get("description", None)

    @property
    def preview_type(self):
        # type: () -> Optional[str]
        """This sets the type of preview that shows up on the task.
        The possible values are: automatic, noPreview, checklist, description, reference.
        When set to automatic the displayed preview is chosen by the app viewing the task.
        """
        return self.properties.get("previewType", None)

    @property
    def references(self):
        # type: () -> PlannerExternalReferences
        """
        The collection of references on the task.
        """
        return self.properties.get("references", PlannerExternalReferences())
