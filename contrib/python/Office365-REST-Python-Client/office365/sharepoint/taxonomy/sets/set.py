from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.taxonomy.item import TaxonomyItem
from office365.sharepoint.taxonomy.item_collection import TaxonomyItemCollection
from office365.sharepoint.taxonomy.sets.name import LocalizedName
from office365.sharepoint.taxonomy.terms.term import Term


class TermSet(TaxonomyItem):
    """Represents a hierarchical or flat set of Term objects known as a 'TermSet'."""

    def __repr__(self):
        if self.is_property_available("localizedNames"):
            return repr(self.localized_names[0])
        else:
            return self.entity_type_name

    @property
    def localized_names(self):
        return self.properties.get(
            "localizedNames", ClientValueCollection(LocalizedName)
        )

    @property
    def terms(self):
        """Gets a collection of the child Term objects"""
        return self.properties.get(
            "terms",
            TaxonomyItemCollection(
                self.context, Term, ResourcePath("terms", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "localizedNames": self.localized_names,
            }
            default_value = property_mapping.get(name, None)
        return super(TermSet, self).get_property(name, default_value)
