from typing import TYPE_CHECKING, Any, Optional, Type, TypeVar

from typing_extensions import Self

from office365.entity import Entity
from office365.runtime.client_object_collection import ClientObjectCollection
from office365.runtime.compat import is_string_type
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v4.entity import EntityPath
from office365.runtime.queries.create_entity import CreateEntityQuery

if TYPE_CHECKING:
    from office365.graph_client import GraphClient

T = TypeVar("T")


class EntityCollection(ClientObjectCollection[T]):
    """A collection container which represents a named collections of entities"""

    def __init__(self, context, item_type, resource_path=None, parent=None):
        # type: (GraphClient, Type[T], Optional[ResourcePath], Optional[Entity]) -> None
        super(EntityCollection, self).__init__(
            context, item_type, resource_path, parent
        )
        self._delta_request_url = None

    def token(self, value):
        """
        Apply delta query

        :param str value: If unspecified, enumerates the hierarchy's current state. If latest, returns empty
            response with latest delta token. If a previous delta token, returns new state since that token.
        """
        self.query_options.custom["token"] = value
        return self

    def __getitem__(self, key):
        # type: (int | str) -> T
        """
        :param key: key is used to address an entity by either an index or by identifier
        :type key: int or str
        """
        if isinstance(key, int):
            return super(EntityCollection, self).__getitem__(key)
        elif is_string_type(key):
            return self.create_typed_object(
                resource_path=EntityPath(key, self.resource_path)
            )
        else:
            raise ValueError(
                "Invalid key: expected either an entity index [int] or identifier [str]"
            )

    def add(self, **kwargs):
        # type: (Any) -> T
        """Creates an entity and prepares the query"""
        return_type = self.create_typed_object(
            kwargs, EntityPath(None, self.resource_path)
        )
        self.add_child(return_type)
        qry = CreateEntityQuery(self, return_type, return_type)
        self.context.add_query(qry)
        return return_type

    def create_typed_object(self, initial_properties=None, resource_path=None):
        # type: (Optional[dict], Optional[ResourcePath]) -> T
        if resource_path is None:
            resource_path = EntityPath(None, self.resource_path)
        return super(EntityCollection, self).create_typed_object(
            initial_properties, resource_path
        )

    def set_property(self, key, value, persist_changes=False):
        # type: (str | int, dict, bool) -> Self
        if key == self.context.pending_request().json_format.collection_delta:
            self._delta_request_url = value
        else:
            super(EntityCollection, self).set_property(key, value, persist_changes)
        return self

    @property
    def context(self):
        # type: () -> GraphClient
        return self._context
