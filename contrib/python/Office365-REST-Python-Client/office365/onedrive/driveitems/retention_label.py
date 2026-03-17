from datetime import datetime
from typing import Optional

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.onedrive.driveitems.retention_label_settings import (
    RetentionLabelSettings,
)


class ItemRetentionLabel(Entity):
    """
    Groups retention and compliance-related properties on an item into a single structure.
    Currently, supported only for driveItem.
    """

    @property
    def is_label_applied_explicitly(self):
        # type: () -> Optional[bool]
        """Specifies whether the label is applied explicitly on the item.
        True indicates that the label is applied explicitly; otherwise, the label is inherited from its parent.
        Read-only."""
        return self.properties.get("isLabelAppliedExplicitly", None)

    @property
    def label_applied_by(self):
        # type: () -> Optional[IdentitySet]
        """Identity of the user who applied the label. Read-only."""
        return self.properties.get("labelAppliedBy", IdentitySet())

    @property
    def label_applied_datetime(self):
        # type: () -> Optional[datetime]
        """The date and time when the label was applied on the item.
        The timestamp type represents date and time information using ISO 8601 format and is always in UTC.
        """
        return self.properties.get("labelAppliedDateTime", datetime.min)

    @property
    def name(self):
        # type: () -> Optional[str]
        """The retention label on the document. Read-write."""
        return self.properties.get("name", None)

    @property
    def retention_settings(self):
        # type: () -> RetentionLabelSettings
        """The retention settings enforced on the item. Read-write."""
        return self.properties.get("retentionSettings", RetentionLabelSettings())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "labelAppliedBy": self.label_applied_by,
                "retentionSettings": self.retention_settings,
            }
            default_value = property_mapping.get(name, None)
        return super(ItemRetentionLabel, self).get_property(name, default_value)
