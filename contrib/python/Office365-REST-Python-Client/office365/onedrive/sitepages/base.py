from typing import Optional

from office365.onedrive.base_item import BaseItem
from office365.onedrive.driveitems.publication_facet import PublicationFacet


class BaseSitePage(BaseItem):
    """An abstract type that represents a page in the site page library."""

    def __repr__(self):
        return self.name or self.entity_type_name

    @property
    def publishing_state(self):
        # type: () -> Optional[str]
        """The publishing status and the MM.mm version of the page."""
        return self.properties.get("publishingState", PublicationFacet())

    @property
    def page_layout(self):
        # type: () -> Optional[str]
        """
        The name of the page layout of the page.
        The possible values are: microsoftReserved, article, home, unknownFutureValue.
        """
        return self.properties.get("pageLayout", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """Title of the sitePage."""
        return self.properties.get("title", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "publishingState": self.publishing_state,
            }
            default_value = property_mapping.get(name, None)
        return super(BaseSitePage, self).get_property(name, default_value)
