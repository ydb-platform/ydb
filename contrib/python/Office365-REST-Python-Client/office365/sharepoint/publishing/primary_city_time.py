from typing import Optional

from office365.sharepoint.entity import Entity


class PrimaryCityTime(Entity):
    """Represents the date and time, in UTC, of the geographic location."""

    @property
    def location(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("Location", None)

    @property
    def time(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("Time", None)

    @property
    def utc_offset(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("UtcOffset", None)
