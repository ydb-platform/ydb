from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.publishing.pages.reposts.repost import RepostPage


class RepostPageCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(RepostPageCollection, self).__init__(context, RepostPage, resource_path)

    def is_content_type_available(self):
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "IsContentTypeAvailable", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
