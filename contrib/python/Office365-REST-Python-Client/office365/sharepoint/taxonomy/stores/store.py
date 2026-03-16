from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.taxonomy.groups.collection import TermGroupCollection
from office365.sharepoint.taxonomy.item import TaxonomyItem
from office365.sharepoint.taxonomy.item_collection import TaxonomyItemCollection
from office365.sharepoint.taxonomy.sets.collection import TermSetCollection
from office365.sharepoint.taxonomy.terms.term import Term


class TermStore(TaxonomyItem):
    """Represents a hierarchical or flat set of Term objects known as a 'TermSet'."""

    def get_term_sets_by_name(self, label, lcid=1033):
        """
        This method retrieves a collection of all TermSet objects in this TermStore
        that the current user has permissions to read that have a matching TermSet name in the provided LCID.
        """
        return_type = TermSetCollection(self.context)

        def _sets_loaded(sets):
            # type: (TermSetCollection) -> None
            [return_type.add_child(ts) for ts in sets]

        def _groups_loaded(col):
            # type: (TermGroupCollection) -> None
            [
                grp.get_term_sets_by_name(label, lcid).after_execute(_sets_loaded)
                for grp in col
            ]

        self.term_groups.get().after_execute(_groups_loaded)
        return return_type

    def search_term(self, label, set_id=None, parent_term_id=None, language_tag=None):
        """
        Search term by name

        :param str label:
        :param str set_id:
        :param str or None parent_term_id:
        :param str or None language_tag:
        """
        return_type = TaxonomyItemCollection[Term](
            self.context, Term, self.resource_path
        )
        params = {
            "label": label,
            "setId": set_id,
            "parentTermId": parent_term_id,
            "languageTag": language_tag,
        }
        qry = FunctionQuery(self, "searchTerm", params, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def default_language_tag(self):
        # type: () -> Optional[str]
        """Gets or sets the LCID of the default working language."""
        return self.properties.get("defaultLanguageTag", None)

    @property
    def language_tags(self):
        """Gets an integer collection of LCIDs."""
        return self.properties.get("languageTags", StringCollection())

    @property
    def term_groups(self):
        # type: () -> TermGroupCollection
        """Gets a collection of the child Group objects"""
        return self.properties.get(
            "termGroups",
            TermGroupCollection(
                self.context, ResourcePath("termGroups", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "termGroups": self.term_groups,
                "languageTags": self.language_tags,
            }
            default_value = property_mapping.get(name, None)
        return super(TermStore, self).get_property(name, default_value)
