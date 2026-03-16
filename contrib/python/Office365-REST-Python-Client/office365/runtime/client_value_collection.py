import uuid
from typing import Any, Dict, Generic, Iterator, List, Optional, Type, TypeVar

from typing_extensions import Self

from office365.runtime.client_value import ClientValue
from office365.runtime.odata.json_format import ODataJsonFormat
from office365.runtime.odata.type import ODataType
from office365.runtime.odata.v3.json_light_format import JsonLightFormat

T = TypeVar("T")


class ClientValueCollection(ClientValue, Generic[T]):
    def __init__(self, item_type, initial_values=None):
        # type: (Type[T], Optional[List | Dict]) -> None
        super(ClientValueCollection, self).__init__()
        if initial_values is None:
            initial_values = []
        self._data = initial_values  # type: list[T]
        self._item_type = item_type

    def add(self, value):
        # type: (T) -> Self
        self._data.append(value)
        return self

    def __getitem__(self, index):
        # type: (int) -> T
        return self._data[index]

    def __iter__(self):
        # type: () -> Iterator[T]
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return repr(self._data)

    def to_json(self, json_format=None):
        # type: (ODataJsonFormat) -> list
        """Serializes a client value's collection"""
        json = [v for v in self]
        for i, v in enumerate(json):
            if isinstance(v, ClientValue):
                json[i] = v.to_json(json_format)
            elif isinstance(v, uuid.UUID):
                json[i] = str(v)
        if (
            isinstance(json_format, JsonLightFormat)
            and json_format.include_control_information
        ):
            json = {
                json_format.collection: json,
                json_format.metadata_type: {"type": self.entity_type_name},
            }
        return json

    def create_typed_value(self, initial_value=None):
        # type: (Optional[T]) -> T
        if initial_value is None:
            return uuid.uuid4() if self._item_type == uuid.UUID else self._item_type()
        elif self._item_type == uuid.UUID:
            return uuid.UUID(initial_value)
        elif issubclass(self._item_type, ClientValue):
            value = self._item_type()
            [value.set_property(k, v, False) for k, v in initial_value.items()]
            return value
        else:
            return initial_value

    def set_property(self, index, value, persist_changes=False):
        # type: (str | int, Any, bool) -> Self
        client_value = self.create_typed_value(value)
        self.add(client_value)
        return self

    @property
    def entity_type_name(self):
        """Returns server type name of value's collection"""
        return "Collection({0})".format(ODataType.resolve_type(self._item_type))
