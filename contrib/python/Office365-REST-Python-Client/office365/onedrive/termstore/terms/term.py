from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.internal.paths.children import ChildrenPath
from office365.onedrive.termstore.terms.description import LocalizedDescription
from office365.onedrive.termstore.terms.label import LocalizedLabel
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath


class Term(Entity):
    """Represents a term used in a term store. A term can be used to represent an object which can then
    be used as a metadata to tag content. Multiple terms can be organized in a hierarchical manner within a set.
    """

    @property
    def descriptions(self):
        """Description about term that is dependent on the languageTag."""
        return self.properties.get(
            "descriptions", ClientValueCollection(LocalizedDescription)
        )

    @property
    def labels(self):
        """Label metadata for a term."""
        return self.properties.get("labels", ClientValueCollection(LocalizedLabel))

    @property
    def created_datetime(self):
        """Timestamp at which the term was created."""
        return self.properties.get("createdDateTime", None)

    @property
    def children(self):
        """Children of current term."""
        return self.properties.get(
            "children",
            EntityCollection(
                self.context,
                Term,
                ChildrenPath(
                    self.resource_path, ResourcePath("terms", self.resource_path.parent)
                ),
            ),
        )

    @property
    def relations(self):
        """To indicate which terms are related to the current term as either pinned or reused."""
        from office365.onedrive.termstore.relation import Relation

        return self.properties.get(
            "relations",
            EntityCollection(
                self.context, Relation, ResourcePath("relations", self.resource_path)
            ),
        )

    @property
    def set(self):
        """The set in which the term is created."""
        from office365.onedrive.termstore.sets.set import Set

        return self.properties.get(
            "set", Set(self.context, ResourcePath("set", self.resource_path))
        )

    @property
    def entity_type_name(self):
        return None
