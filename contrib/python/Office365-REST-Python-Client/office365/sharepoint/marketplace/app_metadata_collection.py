from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.marketplace.app_metadata import CorporateCatalogAppMetadata


class CorporateCatalogAppMetadataCollection(
    EntityCollection[CorporateCatalogAppMetadata]
):
    """Collection of app metadata."""

    def __init__(self, context, resource_path=None):
        super(CorporateCatalogAppMetadataCollection, self).__init__(
            context, CorporateCatalogAppMetadata, resource_path
        )

    def get_by_id(self, app_id):
        """
        Get app metadata by id.

        :param str app_id: The identifier of the app to retrieve.
        """
        return CorporateCatalogAppMetadata(
            self.context, ServiceOperationPath("GetById", [app_id], self.resource_path)
        )

    def get_by_title(self, title):
        """
        Get app metadata by title.

        :param str title: The title of the app to retrieve.
        """
        return self.first("title eq '{0}'".format(title))
