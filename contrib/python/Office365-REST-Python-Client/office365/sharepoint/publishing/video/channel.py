from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class VideoChannel(Entity):
    def get_video_count(self):
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetVideoCount", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Publishing.VideoChannel"
