from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.copilot.file_collection_query_result import (
    CopilotFileCollectionQueryResult,
)
from office365.sharepoint.entity import Entity


class CopilotFileCollection(Entity):
    """ """

    @staticmethod
    def get_working_set_files(context, top=None, order_by=None, skip_token=None):
        """ """
        payload = {"top": top, "orderBy": order_by, "skipToken": skip_token}
        return_type = ClientResult(context, CopilotFileCollectionQueryResult())
        qry = ServiceOperationQuery(
            CopilotFileCollection(context),
            "GetWorkingSetFiles",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type
