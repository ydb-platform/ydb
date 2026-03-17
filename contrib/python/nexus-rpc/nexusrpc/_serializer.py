from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Mapping,
    Optional,
    Protocol,
    Type,
    Union,
)


@dataclass(frozen=True)
class Content:
    """
    A container for a map of headers and a byte array of data.

    It is used by the SDK's Serializer interface implementations.
    """

    headers: Mapping[str, str]
    """
    Header that should include information on how to deserialize this content.
    Headers constructed by the framework always have lower case keys.
    User provided keys are treated case-insensitively.
    """

    data: bytes
    """Request or response data."""


class Serializer(Protocol):
    """
    Serializer is used by the framework to serialize/deserialize input and output.
    """

    def serialize(self, value: Any) -> Union[Content, Awaitable[Content]]:
        """Serialize encodes a value into a Content."""
        ...

    def deserialize(
        self, content: Content, as_type: Optional[Type[Any]] = None
    ) -> Union[Any, Awaitable[Any]]:
        """Deserialize decodes a Content into a value.

        Args:
            content: The content to deserialize.
            as_type: The type to convert the result of deserialization into.
                     Do not attempt type conversion if this is None.
        """
        ...


class LazyValueT(Protocol):
    def consume(
        self, as_type: Optional[Type[Any]] = None
    ) -> Union[Any, Awaitable[Any]]: ...


class LazyValue(LazyValueT):
    """
    A container for a value encoded in an underlying stream.

    It is used to stream inputs and outputs in the various client and server APIs.

    Example:
        .. code-block:: python

            # Creating a LazyValue from raw data
            lazy_input = LazyValue(
                serializer=my_serializer,
                headers={"content-type": "application/json"},
                stream=async_data_stream
            )

            # Using LazyValue with Handler.start_operation
            handler = nexusrpc.handler.Handler([my_service])
            result = await handler.start_operation(ctx, lazy_input)

            # Consuming a LazyValue directly
            deserialized_data = await lazy_input.consume(as_type=MyInputType)
    """

    def __init__(
        self,
        serializer: Serializer,
        headers: Mapping[str, str],
        stream: Optional[AsyncIterable[bytes]] = None,
    ) -> None:
        """
        Args:
            serializer: The serializer to use for consuming the value.
            headers: Headers that include information on how to process the stream's content.
                     Headers constructed by the framework always have lower case keys.
                     User provided keys are treated case-insensitively.
            stream:  Iterable that contains request or response data. None means empty data.
        """
        self.serializer = serializer
        self.headers = headers
        self.stream = stream

    async def consume(self, as_type: Optional[Type[Any]] = None) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        if self.stream is None:
            return await self.serializer.deserialize(
                Content(headers=self.headers, data=b""), as_type=as_type
            )
        elif not isinstance(self.stream, AsyncIterable):
            raise ValueError("When using consume, stream must be an AsyncIterable")

        return await self.serializer.deserialize(
            Content(
                headers=self.headers,
                data=b"".join([c async for c in self.stream]),
            ),
            as_type=as_type,
        )
