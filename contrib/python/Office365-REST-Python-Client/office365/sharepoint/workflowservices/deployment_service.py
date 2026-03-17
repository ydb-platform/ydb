from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.workflow.definition import WorkflowDefinition


class WorkflowDeploymentService(Entity):
    """"""

    def get_definition(self, definition_id):
        """Returns a WorkflowDefinition from the workflow store."""
        return_type = WorkflowDefinition(self.context)
        payload = {"definitionId": definition_id}
        qry = ServiceOperationQuery(
            self, "GetDefinition", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def enumerate_definitions(self, published_only=False):
        """Returns the WorkflowDefinition objects from the workflow store that match the specified parameters."""
        return_type = EntityCollection(self.context, WorkflowDefinition)
        payload = {"publishedOnly": published_only}
        qry = ServiceOperationQuery(
            self, "EnumerateDefinitions", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.WorkflowServices.WorkflowDeploymentService"
