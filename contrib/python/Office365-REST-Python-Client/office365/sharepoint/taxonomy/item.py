from typing import Optional

from office365.runtime.client_object import ClientObject
from office365.runtime.paths.resource_path import ResourcePath


class TaxonomyItem(ClientObject):
    """The TaxonomyItem class is a base class that represents an item in the TermStore (section 3.1.5.23).
    A TaxonomyItem has a name and a unique identifier. It also contains date and time of when the item is created and
    when the item is last modified."""

    @property
    def id(self):
        # type: () -> Optional[str]
        """Gets the Id of the current TaxonomyItem"""
        return self.properties.get("id", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Gets the name of the current TaxonomyItem object"""
        return self.properties.get("name", None)

    @property
    def property_ref_name(self):
        # type: () -> str
        return "id"

    def set_property(self, name, value, persist_changes=True):
        super(TaxonomyItem, self).set_property(name, value, persist_changes)
        if name == self.property_ref_name:
            if self._resource_path is None:
                self._resource_path = ResourcePath(
                    value, self.parent_collection.resource_path
                )
            else:
                self._resource_path.patch(value)
        return self
