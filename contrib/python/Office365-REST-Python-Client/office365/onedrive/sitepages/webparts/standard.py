from typing import Optional

from office365.onedrive.sitepages.webparts.data import WebPartData
from office365.onedrive.sitepages.webparts.web_part import WebPart


class StandardWebPart(WebPart):
    """Represents a standard web part instance on a SharePoint page."""

    @property
    def data(self):
        # type: () -> Optional[WebPartData]
        """Data of the webPart."""
        return self.properties.get("data", WebPartData())
