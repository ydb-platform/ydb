from typing import TypeVar

from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.publishing.pages.metadata import SitePageMetadata

T = TypeVar("T")


class SitePageMetadataCollection(EntityCollection[T]):
    """Specifies a collection of site pages."""

    def get_by_id(self, site_page_id):
        """Gets the site page with the specified ID.
        :param int site_page_id: Specifies the identifier of the site page.
        """
        return SitePageMetadata(
            self.context,
            ServiceOperationPath("GetById", [site_page_id], self.resource_path),
        )
