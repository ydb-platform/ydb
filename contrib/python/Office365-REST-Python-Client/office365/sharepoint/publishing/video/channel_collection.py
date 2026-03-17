from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.publishing.video.channel import VideoChannel


class VideoChannelCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(VideoChannelCollection, self).__init__(
            context, VideoChannel, resource_path
        )
