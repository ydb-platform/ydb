"""Well-known types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Annotated, Any, Optional, cast
from urllib.parse import ParseResult

from pure_protobuf.annotations import Field
from pure_protobuf.helpers._dataclasses import KW_ONLY, SLOTS
from pure_protobuf.helpers.datetime import split_seconds, unsplit_seconds
from pure_protobuf.message import BaseMessage


@dataclass(**KW_ONLY, **SLOTS)
class _TimeSpan(BaseMessage):
    """Base class to represent timespan as whole seconds plus its nanoseconds part."""

    seconds: Annotated[int, Field(1)] = 0
    nanos: Annotated[int, Field(2)] = 0


@dataclass(**KW_ONLY, **SLOTS)
class Timestamp(_TimeSpan):
    """
    Implements the [`Timestamp`](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp)
    well-known type and supports conversion from and to
    [`datetime`](https://docs.python.org/3/library/datetime.html#datetime-objects).
    """

    @classmethod
    def from_datetime(cls, value: datetime) -> Timestamp:
        """Convert the `datetime` to `Timestamp`."""
        seconds, nanos = split_seconds(value.timestamp())
        return cls(seconds=seconds, nanos=nanos)

    def into_datetime(self) -> datetime:
        """Convert to `datetime`."""
        return datetime.fromtimestamp(unsplit_seconds(self.seconds, self.nanos), tz=timezone.utc)


@dataclass(**KW_ONLY, **SLOTS)
class Duration(_TimeSpan):
    """
    Implements the [`#!protobuf Duration`](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration)
    well-known type.
    """

    @classmethod
    def from_timedelta(cls, value: timedelta) -> Duration:
        """Convert the `timedelta` into `Duration`."""
        seconds, nanos = split_seconds(value.total_seconds())
        return cls(seconds=seconds, nanos=nanos)

    def into_timedelta(self) -> timedelta:
        """Convert into `timedelta`."""
        return timedelta(seconds=unsplit_seconds(self.seconds, self.nanos))


@dataclass(**KW_ONLY, **SLOTS)
class Any_(BaseMessage):  # noqa: N801
    """
    Well-known `Any` type.

    See Also:
        - https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/any.proto.
    """

    type_url: Annotated[ParseResult, Field(1)]
    value: Annotated[bytes, Field(2)] = b""

    @classmethod
    def from_message(cls, message: BaseMessage) -> Any_:
        """Convert the message into its `Any_` representation."""

        # noinspection PyArgumentList
        return cls(
            type_url=ParseResult(
                scheme="import",
                netloc=message.__module__,
                path=f"/{type(message).__qualname__}",
                params="",
                query="",
                fragment="",
            ),
            value=bytes(message),
        )

    def into_message(
        self,
        locals_: Optional[Mapping[str, Any]] = None,
        globals_: Optional[Mapping[str, Any]] = None,
    ) -> BaseMessage:
        """
        Reconstructs a message from the current `Any_` representation.

        Args:
            locals_: forwarded to the `__import__` call
            globals_: forwarded to the `__import__` call

        See Also:
            - https://developers.google.com/protocol-buffers/docs/proto3#any
        """

        module = __import__(
            self.type_url.netloc,
            fromlist=[self.type_url.path],
            locals=locals_,
            globals=globals_,
        )
        _, name = self.type_url.path.rsplit("/", maxsplit=1)
        class_ = cast(type[BaseMessage], getattr(module, name))
        return class_.read_from(BytesIO(self.value))
