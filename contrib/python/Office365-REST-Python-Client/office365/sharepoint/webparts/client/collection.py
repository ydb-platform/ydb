from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.webparts.client.webpart import ClientWebPart


class ClientWebPartCollection(EntityCollection[ClientWebPart]):
    """Collection of ClientWebPart representations. It includes all ClientWebParts installed in the SP.Web."""

    def __init__(self, context, resource_path=None):
        super(ClientWebPartCollection, self).__init__(
            context, ClientWebPart, resource_path
        )

    def get_by_id(self, wp_id):
        """Gets the Client web part with the specified ID."""
        return ClientWebPart(
            self.context, ServiceOperationPath("getById", [wp_id], self.resource_path)
        )
