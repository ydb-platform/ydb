from typing import Callable, Generic, Iterator, List, Optional, Type, TypeVar

from typing_extensions import Self

from office365.runtime.client_object import ClientObject
from office365.runtime.client_runtime_context import ClientRuntimeContext
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.odata.json_format import ODataJsonFormat
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.types.event_handler import EventHandler
from office365.runtime.types.exceptions import NotFoundException

T = TypeVar("T")


class ClientObjectCollection(ClientObject, Generic[T]):
    def __init__(self, context, item_type, resource_path=None, parent=None):
        # type: (ClientRuntimeContext, Type[T], Optional[ResourcePath], Optional[ClientObject]) -> None
        """A collection container which represents a named collections of objects."""
        super(ClientObjectCollection, self).__init__(context, resource_path)
        self._data = []  # type: list[T]
        self._item_type = item_type
        self._page_loaded = EventHandler(False)
        self._paged_mode = False
        self._current_pos = None
        self._next_request_url = None
        self._parent = parent

    def clear_state(self):
        """Clears client object collection"""
        if not self._paged_mode:
            self._data = []
        self._next_request_url = None
        self._current_pos = len(self._data)
        return self

    def create_typed_object(self, initial_properties=None, resource_path=None):
        # type: (Optional[dict], Optional[ResourcePath]) -> T
        """Create an object from the item_type."""
        if self._item_type is None:
            raise AttributeError(
                "No class model for entity type '{0}' was found".format(self._item_type)
            )
        if resource_path is None:
            resource_path = ResourcePath(None, self.resource_path)
        client_object = self._item_type(
            context=self.context, resource_path=resource_path
        )  # type: T
        if initial_properties is not None:
            [
                client_object.set_property(k, v)
                for k, v in initial_properties.items()
                if v is not None
            ]
        return client_object

    def set_property(self, key, value, persist_changes=False):
        # type: (str | int, dict, bool) -> Self
        if key == "__nextLinkUrl":
            self._next_request_url = value
        else:
            client_object = self.create_typed_object()
            self.add_child(client_object)
            [
                client_object.set_property(k, v, persist_changes)
                for k, v in value.items()
            ]
        return self

    def add_child(self, client_object):
        # type: (T) -> Self
        """Adds client object into collection"""
        client_object._parent_collection = self
        self._data.append(client_object)
        return self

    def remove_child(self, client_object):
        # type: (T) -> Self
        self._data = [item for item in self._data if item != client_object]
        return self

    def __iter__(self):
        # type: () -> Iterator[T]
        yield from self._data
        if self._paged_mode:
            while self.has_next:
                self._get_next().execute_query()
                next_items = self._data[self._current_pos :]
                yield from next_items

    def __len__(self):
        # type: () -> int
        return len(self._data)

    def __repr__(self):
        # type: () -> str
        return repr(self._data)

    def __getitem__(self, index):
        # type: (int) -> T
        return self._data[index]

    def to_json(self, json_format=None):
        # type: (Optional[ODataJsonFormat]) -> List[dict]
        """Serializes the collection into JSON."""
        return [item.to_json(json_format) for item in self._data]

    def filter(self, expression):
        # type: (str) -> Self
        """
        Allows clients to filter a collection of resources that are addressed by a request URL
        :param str expression: Filter expression, for example: 'Id eq 123'
        """
        self.query_options.filter = expression
        return self

    def order_by(self, value):
        # type: (str) -> Self
        """
        Allows clients to request resources in either ascending order using asc or descending order using desc
        """
        self.query_options.orderBy = value
        return self

    def skip(self, value):
        # type: (int) -> Self
        """
        Requests the number of items in the queried collection that are to be skipped and not included in the result
        """
        self.query_options.skip = value
        return self

    def top(self, value):
        # type: (int) -> Self
        """Specifies the number of items in the queried collection to be included in the result"""
        self.query_options.top = value
        return self

    def paged(self, page_size=None, page_loaded=None):
        # type: (int, Callable[[Self], None] | None) -> Self
        """Retrieves via server-driven paging mode"""
        self._paged_mode = True
        if callable(page_loaded):
            self._page_loaded += page_loaded
        if page_size:
            self.top(page_size)
        return self

    def get(self):
        # type: () -> Self

        def _loaded(col):
            # type: (Self) -> None
            self._page_loaded.notify(self)

        self.context.load(self).after_query_execute(_loaded)
        return self

    def get_all(self, page_size=None, page_loaded=None):
        # type: (int, Callable[[Self], None] | None) -> Self
        """Gets all the items in a collection, regardless of the size."""

        def _page_loaded(col):
            # type: (Self) -> None
            if self.has_next:
                self._get_next().after_execute(_page_loaded)

        self.paged(page_size, page_loaded).get().after_execute(_page_loaded)
        return self

    def _get_next(self):
        # type: () -> Self
        """Submit a request to retrieve next collection of items"""

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.url = self._next_request_url

        return self.get().before_execute(_construct_request)

    def first(self, expression):
        # type: (str) -> T
        """Return the first Entity instance that matches current query
        :param str expression: Filter expression
        """
        return_type = self.create_typed_object()
        self.add_child(return_type)

        def _after_loaded(col):
            # type: (ClientObjectCollection) -> None
            if len(col) < 1:
                message = "Not found for filter: {0}".format(self.query_options.filter)
                raise ValueError(message)
            [
                return_type.set_property(k, v, False)
                for k, v in col[0].properties.items()
            ]

        self.get().filter(expression).top(1).after_execute(_after_loaded)
        return return_type

    def single(self, expression):
        # type: (str) -> T
        """
        Return only one resulting Entity

        :param str expression: Filter expression
        """
        return_type = self.create_typed_object()
        self.add_child(return_type)

        def _after_loaded(col):
            # type: (ClientObjectCollection) -> None
            if len(col) == 0:
                raise NotFoundException(return_type, expression)
            elif len(col) > 1:
                message = "Ambiguous match found for filter: {0}".format(expression)
                raise ValueError(message)
            [
                return_type.set_property(k, v, False)
                for k, v in col[0].properties.items()
            ]

        self.get().filter(expression).top(2).after_execute(_after_loaded)
        return return_type

    @property
    def parent(self):
        # type: () -> ClientObject
        return self._parent

    @property
    def has_next(self):
        # type: () -> bool
        """Determines whether the collection contains a next page of data."""
        return self._next_request_url is not None

    @property
    def current_page(self):
        # type: () -> List[T]
        return self._data[self._current_pos :]

    @property
    def entity_type_name(self):
        # type: () -> str
        """Returns server type name for the collection of entities"""
        if self._entity_type_name is None:
            client_object = self.create_typed_object()
            self._entity_type_name = "Collection({0})".format(
                client_object.entity_type_name
            )
        return self._entity_type_name
