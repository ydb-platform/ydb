from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class VideoServiceDiscoverer(Entity):
    def __init__(self, context):
        super(VideoServiceDiscoverer, self).__init__(
            context, ResourcePath("SP.Publishing.VideoServiceDiscoverer")
        )

    @property
    def video_portal_url(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("VideoPortalUrl", None)
