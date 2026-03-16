import uuid

from office365.entity_collection import EntityCollection
from office365.onedrive.filestorage.container import FileStorageContainer
from office365.runtime.queries.create_entity import CreateEntityQuery


class FileStorageContainerCollection(EntityCollection[FileStorageContainer]):
    """FileStorageContainer collection"""

    def __init__(self, context, resource_path=None):
        super(FileStorageContainerCollection, self).__init__(
            context, FileStorageContainer, resource_path
        )

    def add(self, display_name, container_type_id=uuid.uuid4()):
        """
        Create a new fileStorageContainer object.

        The container type identified by containerTypeId must be registered in the tenant.

        For delegated calls, the calling user is set as the owner of the fileStorageContainer.

        This API is available in the following national cloud deployments.

        :param str display_name: The display name of the container.
        :param str container_type_id: The identifier of the container type.
        """
        return_type = FileStorageContainer(self.context)
        self.add_child(return_type)
        payload = {
            "displayName": display_name,
            "containerTypeId": str(container_type_id),
        }
        qry = CreateEntityQuery(self, payload, return_type)
        self.context.add_query(qry)
        return return_type
