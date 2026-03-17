from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.marketplace.sitecollection.appcatalog.allowed_item import (
    SiteCollectionAppCatalogAllowedItem,
)


class SiteCollectionAppCatalogAllowedItems(
    EntityCollection[SiteCollectionAppCatalogAllowedItem]
):
    """An entry in the site collection app catalog allow list."""

    def __init__(self, context, resource_path=None):
        super(SiteCollectionAppCatalogAllowedItems, self).__init__(
            context, SiteCollectionAppCatalogAllowedItem, resource_path
        )
