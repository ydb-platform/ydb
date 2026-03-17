from typing import Optional

from office365.sharepoint.changes.change import Change


class ChangeField(Change):
    """Specifies a change on a field"""

    @property
    def field_id(self):
        # type: () -> Optional[str]
        """Identifies the changed field"""
        return self.properties.get("FieldId", None)
