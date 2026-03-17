from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class Relation(Entity):
    """
    Represents the relationship between terms in a term store.
    Currently, two types of relationships are supported: pin and reuse.

    In a pin relationship, a term can be pinned under a different term in a different term set.
    In a pinned relationship, new children to the term can only be added in the term set in which the term was created.
    Any change in the hierarchy under the term is reflected across the sets in which the term was pinned.

    The reuse relationship is similar to the pinned relationship except that changes to the reused term can be made
    from any hierarchy in which the term is reused. Also, a change in hierarchy made to the reused term doesn't get
    reflected in the other term sets in which the term is reused.
    """

    @property
    def from_term(self):
        """The from term of the relation. The term from which the relationship is defined.
        A null value would indicate the relation is directly with the set."""
        from office365.onedrive.termstore.terms.term import Term

        return self.properties.get(
            "fromTerm", Term(self.context, ResourcePath("fromTerm", self.resource_path))
        )

    @property
    def to_term(self):
        """The to term of the relation. The term to which the relationship is defined."""
        from office365.onedrive.termstore.terms.term import Term

        return self.properties.get(
            "toTerm", Term(self.context, ResourcePath("toTerm", self.resource_path))
        )

    @property
    def set(self):
        """The set in which the relation is relevant."""
        from office365.onedrive.termstore.sets.set import Set

        return self.properties.get(
            "set", Set(self.context, ResourcePath("set", self.resource_path))
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"fromTerm": self.from_term, "toTerm": self.to_term}
            default_value = property_mapping.get(name, None)
        return super(Relation, self).get_property(name, default_value)
