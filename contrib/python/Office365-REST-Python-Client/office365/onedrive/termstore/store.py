from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.termstore.groups.collection import GroupCollection
from office365.onedrive.termstore.sets.collection import SetCollection
from office365.onedrive.termstore.sets.set import Set
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.types.collections import StringCollection


class Store(Entity):
    """Represents a taxonomy term store."""

    def get_all_term_sets(self):
        """Returns a collection containing a flat list of all TermSet objects."""
        return_type = EntityCollection(self.context, Set)

        def _sets_loaded(sets):
            # type: (SetCollection) -> None
            [return_type.add_child(s) for s in sets]

        def _groups_loaded(groups):
            # type: (GroupCollection) -> None
            [grp.sets.get().after_execute(_sets_loaded) for grp in groups]

        self.groups.get().after_execute(_groups_loaded)

        return return_type

    @property
    def default_language_tag(self):
        # type: () -> Optional[str]
        """Default language of the term store."""
        return self.properties.get("defaultLanguageTag", None)

    @property
    def language_tags(self):
        """List of languages for the term store."""
        return self.properties.get("languageTags", StringCollection())

    @property
    def groups(self):
        # type: () -> GroupCollection
        """Collection of all groups available in the term store."""
        return self.properties.get(
            "groups",
            GroupCollection(self.context, ResourcePath("groups", self.resource_path)),
        )

    @property
    def sets(self):
        """Collection of all sets available in the term store."""
        return self.properties.get(
            "sets",
            SetCollection(self.context, ResourcePath("sets", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"languageTags": self.language_tags}
            default_value = property_mapping.get(name, None)
        return super(Store, self).get_property(name, default_value)
