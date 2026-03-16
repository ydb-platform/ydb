from office365.sharepoint.entity import Entity


class WorkflowInstance(Entity):
    """Represents a workflow instance."""

    @property
    def entity_type_name(self):
        return "SP.WorkflowServices.WorkflowInstance"
