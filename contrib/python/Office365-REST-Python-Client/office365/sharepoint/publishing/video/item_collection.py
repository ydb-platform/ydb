from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.publishing.video.item import VideoItem


class VideoItemCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(VideoItemCollection, self).__init__(context, VideoItem, resource_path)
