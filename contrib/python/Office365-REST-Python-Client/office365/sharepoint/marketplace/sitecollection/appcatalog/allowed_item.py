from typing import Optional

from office365.sharepoint.entity import Entity


class SiteCollectionAppCatalogAllowedItem(Entity):
    """An entry in the site collection app catalog allow list."""

    @property
    def site_id(self):
        # type: () -> Optional[str]
        """The ID of a site collection in the allow list."""
        return self.properties.get("SiteID", None)

    @property
    def absolute_url(self):
        # type: () -> Optional[str]
        """The absolute URL of a site collection in the allow list."""
        return self.properties.get("AbsoluteUrl", None)

    @property
    def property_ref_name(self):
        return "AbsoluteUrl"

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Marketplace.CorporateCuratedGallery.SiteCollectionAppCatalogAllowedItem"
