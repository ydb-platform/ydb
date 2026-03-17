from __future__ import annotations

import abc
from io import BytesIO
from types import UnionType
from typing import Any, ClassVar, Generic, TypeVar, get_args, get_origin

from aiokafka.errors import IncompatibleBrokerVersion

from .struct import Struct
from .types import Array, Int16, Int32, Schema, String, TaggedFields


class RequestHeader_v1(Struct):
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
    )

    def __init__(
        self,
        request: RequestStruct,
        correlation_id: int = 0,
        client_id: str = "aiokafka",
    ) -> None:
        super().__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id
        )


class RequestHeader_v2(Struct):
    # Flexible response / request headers end in field buffer
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
        ("tags", TaggedFields),
    )

    def __init__(
        self,
        request: RequestStruct,
        correlation_id: int = 0,
        client_id: str = "aiokafka",
        tags: dict[int, bytes] | None = None,
    ):
        super().__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id, tags or {}
        )


class ResponseHeader_v0(Struct):
    SCHEMA = Schema(
        ("correlation_id", Int32),
    )


class ResponseHeader_v1(Struct):
    SCHEMA = Schema(
        ("correlation_id", Int32),
        ("tags", TaggedFields),
    )


T = TypeVar("T", bound="RequestStruct")


class Request(abc.ABC, Generic[T]):
    """
    Base class for all the requests classes.
    The aiokafka clients must use children classes of Request
    in order to communicate with the brokers.
    The correct API versions will be negotiated per connection
    basis, so this class is acting as a builder for the final
    RequestStruct sent over the wire.

    When implementing a Request sub-class, you must accept all
    the different attributes in the constructor, then eventually
    raise a IncompatibleBrokerVersion exception if the negotiated
    version doesn't allow some attributes to be sent.

    Type T is used to describe all the possible RequestStruct
    classes supported, ordered by version ascending.

    Attributes
    ----------
    API_KEY : int
        The unique API key identifying the request.
    ALLOW_UNKNOWN_API_VERSION: bool
        If true, the request could be used without knowing
        the API version
    """

    API_KEY: ClassVar[int]
    ALLOW_UNKNOWN_API_VERSION: ClassVar[bool] = False

    _CLASSES: ClassVar[tuple[type[RequestStruct]]]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not hasattr(cls, "API_KEY"):
            raise TypeError(f"{cls.__name__} must define class attributes 'API_KEY'")
        # To be replaced by typing.get_original_bases when 3.12 is the min version
        if (
            hasattr(cls, "__orig_bases__")
            and (base_classes := cls.__orig_bases__)
            and len(base_classes) == 1
        ):
            generic_type = get_args(base_classes[0])[0]
            if get_origin(generic_type) is UnionType:
                cls._CLASSES = get_args(get_args(base_classes[0])[0])
            else:
                cls._CLASSES = (generic_type,)

        else:
            raise TypeError(f"{cls.__name__} must extend Request")

    @abc.abstractmethod
    def build(self, request_struct_class: type[T]) -> T:
        pass

    def prepare(self, versions: dict[int, tuple[int, int]]) -> T:
        api_key = self.API_KEY
        if api_key not in versions:
            if self.ALLOW_UNKNOWN_API_VERSION:
                return self.build(self._CLASSES[0])  # type: ignore[arg-type]
            raise IncompatibleBrokerVersion(
                f"{self.__class__.__name__} cannot be used "
                "if the API version is unknown"
            )

        min_version, max_version = versions[api_key]
        for req_class in reversed(self._CLASSES):
            if min_version <= req_class.API_VERSION <= max_version:
                return self.build(req_class)  # type: ignore[arg-type]

        raise NotImplementedError(
            f"Support for {self.__class__.__name__} v{min_version} "
            "has not yet been added"
        )


class RequestStruct(Struct, metaclass=abc.ABCMeta):
    """
    Base structure for API requests.

    Attributes
    ----------
    FLEXIBLE_VERSION : bool
        Use request header with flexible tags
    API_KEY : int
        The unique API key identifying the request.
    API_VERSION : int
        Which API version the RequestStruct class is.
    RESPONSE_TYPE : type[Response]
        Class used to parse the response.
    SCHEMA : Schema
        An instance of Schema() representing the request structure.
    """

    FLEXIBLE_VERSION: ClassVar[bool] = False
    API_KEY: ClassVar[int]
    API_VERSION: ClassVar[int]
    RESPONSE_TYPE: ClassVar[type[Response]]
    SCHEMA: ClassVar[Schema]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if (
            not hasattr(cls, "API_KEY")
            or not hasattr(cls, "API_VERSION")
            or not hasattr(cls, "RESPONSE_TYPE")
            or not hasattr(cls, "SCHEMA")
        ):
            raise TypeError(
                f"{cls.__name__} must define class attributes "
                "'API_KEY', 'API_VERSION', 'RESPONSE_TYPE' and 'SCHEMA"
            )

    def to_object(self) -> dict[str, Any]:
        return _to_object(self.SCHEMA, self)

    def build_request_header(
        self, correlation_id: int, client_id: str
    ) -> RequestHeader_v1 | RequestHeader_v2:
        if self.FLEXIBLE_VERSION:
            return RequestHeader_v2(
                self, correlation_id=correlation_id, client_id=client_id
            )
        return RequestHeader_v1(
            self, correlation_id=correlation_id, client_id=client_id
        )

    def parse_response_header(
        self, read_buffer: BytesIO | bytes
    ) -> ResponseHeader_v0 | ResponseHeader_v1:
        if self.FLEXIBLE_VERSION:
            return ResponseHeader_v1.decode(read_buffer)
        return ResponseHeader_v0.decode(read_buffer)


class Response(Struct, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response"""

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version"""

    def to_object(self) -> dict[str, Any]:
        return _to_object(self.SCHEMA, self)


def _to_object(schema: Schema, data: Struct | dict[int, Any]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for idx, (name, _type) in enumerate(zip(schema.names, schema.fields, strict=False)):
        if isinstance(data, Struct):
            val = data.get_item(name)
        else:
            val = data[idx]

        if isinstance(_type, Schema):
            obj[name] = _to_object(_type, val)
        elif isinstance(_type, Array):
            if isinstance(_type.array_of, Schema):
                obj[name] = [_to_object(_type.array_of, x) for x in val]
            else:
                obj[name] = val
        else:
            obj[name] = val

    return obj
