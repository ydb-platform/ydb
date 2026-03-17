from typing import TYPE_CHECKING, Optional, Type, TypeVar

from office365.runtime.client_object_collection import ClientObjectCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v3.entity import EntityPath
from office365.sharepoint.entity import Entity

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext

T = TypeVar("T")


class EntityCollection(ClientObjectCollection[T]):
    """SharePoint's entity set"""

    def __init__(self, context, item_type, resource_path=None, parent=None):
        # type: (ClientContext, Type[T], Optional[ResourcePath], Optional[Entity]) -> None
        super(EntityCollection, self).__init__(
            context, item_type, resource_path, parent
        )

    def create_typed_object(self, initial_properties=None, resource_path=None):
        # type: (Optional[dict], Optional[ResourcePath]) -> T
        if resource_path is None:
            resource_path = EntityPath(None, self.resource_path)
        return super(EntityCollection, self).create_typed_object(
            initial_properties, resource_path
        )

    @property
    def context(self):
        # type: () -> ClientContext
        return self._context
