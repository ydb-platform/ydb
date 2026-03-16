from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.workflowservices.instance import WorkflowInstance


class WorkflowInstanceService(Entity):
    """Manages and reads workflow instances from the workflow host."""

    def enumerate_instances_for_site(self):
        """
        Returns the site workflow instances for the current site.
        """
        return_type = EntityCollection(self.context, WorkflowInstance)
        qry = ServiceOperationQuery(
            self, "EnumerateInstancesForSite", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.WorkflowServices.WorkflowInstanceService"
