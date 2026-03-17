from datetime import datetime
from typing import Optional

from office365.onenote.entity_base_model import OnenoteEntityBaseModel


class OnenoteEntitySchemaObjectModel(OnenoteEntityBaseModel):
    """This is a base type for OneNote entities."""

    @property
    def created_datetime(self):
        # type: () -> Optional[datetime]
        """
        The date and time when the page was created. The timestamp represents date and time information using
        ISO 8601 format and is always in UTC time. For example, midnight UTC on Jan 1, 2014 is 2014-01-01T00:00:00Z.
        """
        return self.properties.get("createdDateTime", datetime.min)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdDateTime": self.created_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(OnenoteEntitySchemaObjectModel, self).get_property(
            name, default_value
        )
