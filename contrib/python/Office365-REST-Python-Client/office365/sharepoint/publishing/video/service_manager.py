from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.publishing.video.channel_collection import (
    VideoChannelCollection,
)


class VideoServiceManager(Entity):
    def __init__(self, context):
        super(VideoServiceManager, self).__init__(
            context, ResourcePath("SP.Publishing.VideoServiceManager")
        )

    def get_channels(self, start_index=0, limit=None):
        return_type = VideoChannelCollection(self.context)
        params = {"startIndex": start_index, "limit": limit}
        qry = ServiceOperationQuery(
            self, "GetChannels", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
