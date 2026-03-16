import datetime

from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.taxonomy.item import TaxonomyItem
from office365.sharepoint.taxonomy.terms.label import Label


class Term(TaxonomyItem):
    """Represents a Term or a Keyword in a managed metadata hierarchy."""

    def __repr__(self):
        return "{0}".format(self.id)

    @property
    def is_deprecated(self):
        # type: () -> bool
        """"""
        return self.properties.get("isDeprecated", None)

    @property
    def children_count(self):
        # type: () -> int
        """"""
        return self.properties.get("childrenCount", None)

    @property
    def created_datetime(self):
        # type: () -> datetime.datetime
        """"""
        return self.properties.get("createdDateTime", None)

    @property
    def labels(self):
        """Gets a collection of Label objects for the current Term object."""
        return self.properties.get("labels", ClientValueCollection(Label))

    @property
    def parent(self):
        return self.properties.get(
            "parent",
            Term(self.context, ResourcePath("parent", self.resource_path)),
        )
