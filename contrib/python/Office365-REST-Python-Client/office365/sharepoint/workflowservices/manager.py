from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class WorkflowServicesManager(Entity):
    """Describes the workflow host configuration states and provides service objects that interact with the workflow."""

    def get_workflow_instance_service(self):
        """Returns the WorkflowInstanceService (manages and reads workflow instances from the workflow host),
        which manages workflow instances."""
        from office365.sharepoint.workflowservices.instance_service import (
            WorkflowInstanceService,
        )

        return_type = WorkflowInstanceService(self.context)
        qry = ServiceOperationQuery(
            self, "GetWorkflowInstanceService", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def current(context):
        """
        Specifies the current instance for the SP.TenantSettings.

        :type context: office365.sharepoint.client_context.ClientContext
        """
        return WorkflowServicesManager(
            context, ResourcePath("SP.WorkflowServices.WorkflowServicesManager.Current")
        )

    @property
    def entity_type_name(self):
        return "SP.WorkflowServices.WorkflowServicesManager"
