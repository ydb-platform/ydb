from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.internal.paths.children import ChildrenPath
from office365.onedrive.termstore.relation import Relation
from office365.onedrive.termstore.sets.name import LocalizedName
from office365.onedrive.termstore.terms.collection import TermCollection
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath


class Set(Entity):
    """
    Represents the set used in a term store. The set represents a unit which contains a collection of hierarchical
    terms. A group can contain multiple sets.
    """

    def __repr__(self):
        return repr(self.localized_names)

    @property
    def children(self):
        # type: () -> TermCollection
        """Children terms of set in term store."""
        return self.properties.get(
            "children",
            TermCollection(
                self.context,
                ChildrenPath(self.resource_path, self.terms.resource_path),
                self,
            ),
        )

    @property
    def localized_names(self):
        # type: () -> ClientValueCollection[LocalizedName]
        """"""
        return self.properties.get(
            "localizedNames", ClientValueCollection(LocalizedName)
        )

    @property
    def parent_group(self):
        """The parent group that contains the set."""
        from office365.onedrive.termstore.groups.group import Group

        return self.properties.get(
            "parentGroup",
            Group(self.context, ResourcePath("parentGroup", self.resource_path)),
        )

    @property
    def relations(self):
        # type: () -> EntityCollection[Relation]
        """Indicates which terms have been pinned or reused directly under the set."""
        return self.properties.get(
            "relations",
            EntityCollection(
                self.context, Relation, ResourcePath("relations", self.resource_path)
            ),
        )

    @property
    def terms(self):
        # type: () -> TermCollection
        """All the terms under the set."""
        return self.properties.get(
            "terms",
            TermCollection(
                self.context, ResourcePath("terms", self.resource_path), self
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "localizedNames": self.localized_names,
                "parentGroup": self.parent_group,
            }
            default_value = property_mapping.get(name, None)
        return super(Set, self).get_property(name, default_value)

    @property
    def entity_type_name(self):
        return None
