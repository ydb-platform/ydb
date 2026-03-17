from typing import TYPE_CHECKING, Optional

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.flows.connector_result import ConnectorResult

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class FormsCustomization(Entity):

    @staticmethod
    def can_customize_forms(context, list_name, return_type=None):
        # type: (ClientContext, str, Optional[ConnectorResult]) -> ConnectorResult
        """"""
        if return_type is None:
            return_type = ConnectorResult(context)
        payload = {"listName": list_name}
        qry = ServiceOperationQuery(
            FormsCustomization(context),
            "CanCustomizeForms",
            None,
            payload,
            None,
            return_type,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Internal.FormsCustomization"
