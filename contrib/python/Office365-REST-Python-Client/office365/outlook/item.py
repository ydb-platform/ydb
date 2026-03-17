import datetime
from typing import Optional

from office365.entity import Entity
from office365.runtime.types.collections import StringCollection


class OutlookItem(Entity):
    @property
    def change_key(self):
        # type: () -> Optional[str]
        """
        Identifies the version of the item. Every time the item is changed, changeKey changes as well.
        This allows Exchange to apply changes to the correct version of the object.
        """
        return self.properties.get("ChangeKey", None)

    @property
    def categories(self):
        """The categories associated with the item"""
        return self.properties.get("categories", StringCollection())

    @property
    def created_datetime(self):
        """
        The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time.
        For example, midnight UTC on Jan 1, 2014 is 2014-01-01T00:00:00Z
        """
        return self.properties.get("createdDateTime", datetime.datetime.min)

    @property
    def last_modified_datetime(self):
        """
        The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time.
        For example, midnight UTC on Jan 1, 2014 is 2014-01-01T00:00:00Z
        """
        return self.properties.get("lastModifiedDateTime", datetime.datetime.min)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdDateTime": self.created_datetime,
                "lastModifiedDateTime": self.last_modified_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(OutlookItem, self).get_property(name, default_value)
