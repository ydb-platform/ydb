from typing import TYPE_CHECKING, Optional, Type, TypeVar

from office365.delta_path import DeltaPath
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath

T = TypeVar("T")

if TYPE_CHECKING:
    from office365.graph_client import GraphClient


class DeltaCollection(EntityCollection[T]):
    def __init__(self, context, item_type, resource_path=None, parent=None):
        # type: (GraphClient, Type[T], Optional[ResourcePath], Optional[Entity]) -> None
        super(DeltaCollection, self).__init__(context, item_type, resource_path, parent)

    def change_type(self, type_name):
        """
        Specifies a custom query option to filter the delta response based on the type of change.

        :param str type_name: Supported values are created, updated or deleted.
        """
        self.query_options.custom["changeType"] = type_name
        return self

    @property
    def delta(self):
        # type: () -> DeltaCollection[T]
        """
        Get newly created, updated, or deleted entities (changes)
        """
        return self.properties.get(
            "delta",
            DeltaCollection(
                self.context, self._item_type, DeltaPath(self.resource_path)
            ),
        )
