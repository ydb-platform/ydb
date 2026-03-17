from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.taxonomy.item import TaxonomyItem
from office365.sharepoint.taxonomy.item_collection import TaxonomyItemCollection
from office365.sharepoint.taxonomy.sets.collection import TermSetCollection
from office365.sharepoint.taxonomy.sets.set import TermSet


class TermGroup(TaxonomyItem):
    """Represents the top-level container in a TermStore object."""

    def __str__(self):
        return self.display_name or self.entity_type_name

    def get_term_sets_by_name(self, label, lcid=None):
        """
        Search term set by name

        :param str label: The name of the TermSet object.
        :param int lcid: LCID of the language.
        """
        return_type = TermSetCollection(self.context)

        def _sets_loaded(col):
            # type: (TermSetCollection) -> None
            [
                return_type.add_child(ts)
                for ts in col
                if str(ts.localized_names[0]) == label
            ]

        def _group_resolved():
            self.term_sets.get().after_execute(_sets_loaded)

        self.ensure_property("id", _group_resolved)
        return return_type

    @property
    def term_sets(self):
        # type: () -> TaxonomyItemCollection[TermSet]
        """
        Gets a collection of the child TermSet instances of this TermGroup object.
        """
        return self.properties.get(
            "termSets",
            TaxonomyItemCollection(
                self.context, TermSet, ResourcePath("termSets", self.resource_path)
            ),
        )

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """Gets the name of the Term Group"""
        return self.properties.get("displayName", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"termSets": self.term_sets}
            default_value = property_mapping.get(name, None)
        return super(TermGroup, self).get_property(name, default_value)
