from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class RecentFileCollection(Entity):
    def __init__(self, context):
        super(RecentFileCollection, self).__init__(
            context, ResourcePath("SP.RecentFileCollection")
        )

    @staticmethod
    def get_recent_files(context, top):
        """
        :type context: office365.sharepoint.client_context.ClientContext
        :param int top:
        """
        return_type = ClientResult(context, str())
        payload = {"top": top}
        binding_type = RecentFileCollection(context)
        qry = ServiceOperationQuery(
            binding_type, "GetRecentFiles", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type
