from typing import Optional

from office365.sharepoint.entity import Entity


class EmbedDataV1(Entity):
    """Represents embedded meta data of the page."""

    def url(self):
        # type: () -> Optional[str]
        """The URL of the page."""
        return self.properties.get("Url", None)

    def video_id(self):
        # type: () -> Optional[str]
        """If the page represents a video, the value will be video id."""
        return self.properties.get("VideoId", None)

    def web_id(self):
        # type: () -> Optional[str]
        """If the page belongs to website, the value will be website id, otherwise the value will be empty."""
        return self.properties.get("WebId", None)
