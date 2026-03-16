from typing import Optional

from office365.sharepoint.changes.change import Change


class ChangeAlert(Change):
    """Specifies a change from an alert."""

    @property
    def alert_id(self):
        # type: () -> Optional[str]
        """Identifies the changed alert."""
        return self.properties.get("AlertId", None)
