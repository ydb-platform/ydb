from typing import Optional

from office365.entity import Entity
from office365.runtime.types.collections import StringCollection


class SingleValueLegacyExtendedProperty(Entity):
    """An extended property that contains a single value."""

    @property
    def value(self):
        # type: () -> Optional[str]
        """A property value."""
        return self.properties.get("value", None)


class MultiValueLegacyExtendedProperty(Entity):
    """An extended property that contains a collection of values."""

    @property
    def value(self):
        """A collection of property values."""
        return self.properties.get("value", StringCollection())
