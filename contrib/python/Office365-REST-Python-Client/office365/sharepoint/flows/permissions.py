from typing import TYPE_CHECKING, Optional

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.flows.connector_result import ConnectorResult

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class FlowPermissions(Entity):

    @staticmethod
    def get_flow_permission_level_on_list(context, list_name, return_type=None):
        # type: (ClientContext, str, Optional[ConnectorResult]) -> ConnectorResult
        """ """
        if return_type is None:
            return_type = ConnectorResult(context)
        payload = {"listName": list_name}
        qry = ServiceOperationQuery(
            FlowPermissions(context),
            "GetFlowPermissionLevelOnList",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Internal.FlowPermissions"
