"""Payload converter types and implementations for data conversion."""

from __future__ import annotations

import collections
import collections.abc
import dataclasses
import functools
import inspect
import json
import sys
import typing
import uuid
import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from enum import IntEnum
from itertools import zip_longest
from types import UnionType
from typing import (
    Any,
    ClassVar,
    Generic,
    Literal,
    NewType,
    TypeVar,
    get_type_hints,
    overload,
)

import google.protobuf.json_format
import google.protobuf.message
import google.protobuf.symbol_database
import typing_extensions
from typing_extensions import Self

import temporalio.api.common.v1
import temporalio.common
import temporalio.types

if sys.version_info < (3, 11):
    # Python's datetime.fromisoformat doesn't support certain formats pre-3.11
    from dateutil import parser  # type: ignore
# StrEnum is available in 3.11+
if sys.version_info >= (3, 11):
    from enum import StrEnum  # type: ignore[reportUnreachable]

from temporalio.converter._serialization_context import (
    SerializationContext,
    WithSerializationContext,
)

_sym_db = google.protobuf.symbol_database.Default()
ValueT = TypeVar("ValueT")
TransferTypeT = TypeVar("TransferTypeT")
_TRANSFER_TYPE_CONVERTER_ATTR = "__temporal_transfer_type_converter"


class TransferTypeConverter(Generic[ValueT, TransferTypeT], ABC):
    """Converter between a user-facing value and a transfer type value.

    .. warning::
        This API is experimental and subject to change.
    """

    transfer_type: type[TransferTypeT] | None = None
    """Optional type hint for the transfer type to use when decoding payloads.

    .. warning::
        This API is experimental and subject to change.
    """

    @abstractmethod
    def to_transfer_type(self, value: ValueT) -> TransferTypeT:
        """Convert a user-facing value to its transfer type value.

        .. warning::
            This API is experimental and subject to change.
        """
        raise NotImplementedError

    @abstractmethod
    def from_transfer_type(
        self, value: TransferTypeT, type_hint: type[ValueT]
    ) -> ValueT:
        """Convert a transfer type value to its user-facing value.

        ``type_hint`` is the requested user-facing type, including concrete
        generic arguments.

        .. warning::
            This API is experimental and subject to change.
        """
        raise NotImplementedError


class _TransferTypeConvertibleDecorator(Generic[ValueT, TransferTypeT]):
    def __init__(
        self, converter_type: type[TransferTypeConverter[ValueT, TransferTypeT]]
    ) -> None:
        self._converter_type = converter_type

    def __call__(self, cls: type[ValueT]) -> type[ValueT]:
        if hasattr(cls, _TRANSFER_TYPE_CONVERTER_ATTR):
            raise TypeError("class already has a transfer type converter")
        setattr(cls, _TRANSFER_TYPE_CONVERTER_ATTR, self._converter_type())
        return cls


def transfer_type_convertible(
    converter_type: type[TransferTypeConverter[ValueT, TransferTypeT]],
) -> _TransferTypeConvertibleDecorator[ValueT, TransferTypeT]:
    """Decorate a class with a transfer type converter class.

    .. warning::
        This API is experimental and subject to change.
    """
    return _TransferTypeConvertibleDecorator(converter_type)


def _get_transfer_type_converter(
    value_type: object,
) -> TransferTypeConverter[Any, Any] | None:
    converter = getattr(value_type, _TRANSFER_TYPE_CONVERTER_ATTR, None)
    if isinstance(converter, TransferTypeConverter):
        return converter
    return None


class PayloadConverter(ABC):
    """Base payload converter to/from multiple payloads/values."""

    default: ClassVar[PayloadConverter]
    """Default payload converter."""

    @abstractmethod
    def to_payloads(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
        """Encode values into payloads.

        Implementers are expected to just return the payload for
        :py:class:`temporalio.common.RawValue`.

        Args:
            values: Values to be converted.

        Returns:
            Converted payloads. Note, this does not have to be the same number
            as values given, but must be at least one and cannot be more than
            was given.

        Raises:
            Exception: Any issue during conversion.
        """
        raise NotImplementedError

    @abstractmethod
    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[Any]:
        """Decode payloads into values.

        Implementers are expected to treat a type hint of
        :py:class:`temporalio.common.RawValue` as just the raw value.

        Args:
            payloads: Payloads to convert to Python values.
            type_hints: Types that are expected if any. This may not have any
                types if there are no annotations on the target. If this is
                present, it must have the exact same length as payloads even if
                the values are just "object".

        Returns:
            Collection of Python values. Note, this does not have to be the same
            number as values given, but at least one must be present.

        Raises:
            Exception: Any issue during conversion.
        """
        raise NotImplementedError

    def to_payloads_wrapper(
        self, values: Sequence[Any]
    ) -> temporalio.api.common.v1.Payloads:
        """:py:meth:`to_payloads` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.
        """
        return temporalio.api.common.v1.Payloads(payloads=self.to_payloads(values))

    def from_payloads_wrapper(
        self, payloads: temporalio.api.common.v1.Payloads | None
    ) -> list[Any]:
        """:py:meth:`from_payloads` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.
        """
        if not payloads or not payloads.payloads:
            return []
        return self.from_payloads(payloads.payloads)

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload:
        """Convert a single value to a payload.

        This is a shortcut for :py:meth:`to_payloads` with a single-item list
        and result.

        Args:
            value: Value to convert to a single payload.

        Returns:
            Single converted payload.
        """
        return self.to_payloads([value])[0]

    @overload
    def from_payload(self, payload: temporalio.api.common.v1.Payload) -> Any: ...

    @overload
    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type[temporalio.types.AnyType],
    ) -> temporalio.types.AnyType: ...

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """Convert a single payload to a value.

        This is a shortcut for :py:meth:`from_payloads` with a single-item list
        and result.

        Args:
            payload: Payload to convert to value.
            type_hint: Optional type hint to say which type to convert to.

        Returns:
            Single converted value.
        """
        return self.from_payloads([payload], [type_hint] if type_hint else None)[0]


class EncodingPayloadConverter(ABC):
    """Base converter to/from single payload/value with a known encoding for use in CompositePayloadConverter."""

    @property
    @abstractmethod
    def encoding(self) -> str:
        """Encoding for the payload this converter works with."""
        raise NotImplementedError

    @abstractmethod
    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """Encode a single value to a payload or None.

        Args:
            value: Value to be converted.

        Returns:
            Payload of the value or None if unable to convert.

        Raises:
            TypeError: Value is not the expected type.
            ValueError: Value is of the expected type but otherwise incorrect.
            RuntimeError: General error during encoding.
        """
        raise NotImplementedError

    @abstractmethod
    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """Decode a single payload to a Python value or raise exception.

        Args:
            payload: Payload to convert to Python value.
            type_hint: Type that is expected if any. This may not have a type if
                there are no annotations on the target.

        Return:
            The decoded value from the payload. Since the encoding is checked by
            the caller, this should raise an exception if the payload cannot be
            converted.

        Raises:
            RuntimeError: General error during decoding.
        """
        raise NotImplementedError


class CompositePayloadConverter(PayloadConverter, WithSerializationContext):
    """Composite payload converter that delegates to a list of encoding payload converters.

    Encoding/decoding are attempted on each payload converter successively until
    it succeeds.

    Attributes:
        converters: List of payload converters to delegate to, in order.
    """

    converters: Mapping[bytes, EncodingPayloadConverter]

    def __init__(self, *converters: EncodingPayloadConverter) -> None:
        """Initializes the data converter.

        Args:
            converters: Payload converters to delegate to, in order.
        """
        self._set_converters(*converters)

    def _set_converters(self, *converters: EncodingPayloadConverter) -> None:
        self.converters = {c.encoding.encode(): c for c in converters}

    def to_payloads(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
        """Encode values trying each converter.

        See base class. Always returns the same number of payloads as values.

        Raises:
            RuntimeError: No known converter
        """
        payloads = []
        for index, value in enumerate(values):
            # We intentionally attempt these serially just in case a stateful
            # converter may rely on the previous values
            payload = None
            # RawValue should just pass through
            if isinstance(value, temporalio.common.RawValue):
                payload = value.payload
            else:
                for converter in self.converters.values():
                    payload = converter.to_payload(value)
                    if payload is not None:
                        break
            if payload is None:
                raise RuntimeError(
                    f"Value at index {index} of type {type(value)} has no known converter"
                )
            payloads.append(payload)
        return payloads

    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[Any]:
        """Decode values trying each converter.

        See base class. Always returns the same number of values as payloads.

        Raises:
            KeyError: Unknown payload encoding
            RuntimeError: Error during decode
        """
        values = []
        type_hints = type_hints or []
        for index, (payload, type_hint) in enumerate(zip_longest(payloads, type_hints)):
            # Raw value should just wrap
            if type_hint == temporalio.common.RawValue:
                values.append(temporalio.common.RawValue(payload))
                continue
            encoding = payload.metadata.get("encoding", b"<unknown>")
            converter = self.converters.get(encoding)
            if converter is None:
                raise KeyError(f"Unknown payload encoding {encoding.decode()}")
            try:
                values.append(converter.from_payload(payload, type_hint))
            except RuntimeError as err:
                raise RuntimeError(
                    f"Payload at index {index} with encoding {encoding.decode()} could not be converted"
                ) from err
        return values

    def with_context(self, context: SerializationContext) -> Self:
        """Return a new instance with context set on the component converters.

        If none of the component converters returned new instances, return self.
        """
        converters = self.get_converters_with_context(context)
        if converters is None:
            return self
        new_instance = type(self)()  # Must have a nullary constructor
        new_instance._set_converters(*converters)
        return new_instance

    def get_converters_with_context(
        self, context: SerializationContext
    ) -> list[EncodingPayloadConverter] | None:
        """Return converter instances with context set.

        If no converter uses context, return None.
        """
        if not self._any_converter_takes_context:
            return None
        converters: list[EncodingPayloadConverter] = []
        any_with_context = False
        for c in self.converters.values():
            if isinstance(c, WithSerializationContext):
                converters.append(c.with_context(context))
                any_with_context |= converters[-1] is not c
            else:
                converters.append(c)

        return converters if any_with_context else None

    @functools.cached_property
    def _any_converter_takes_context(self) -> bool:
        return any(
            isinstance(c, WithSerializationContext) for c in self.converters.values()
        )


class DefaultPayloadConverter(CompositePayloadConverter):
    """Default payload converter compatible with other Temporal SDKs.

    This handles None, bytes, all protobuf message types, and any type that
    :py:func:`json.dump` accepts. A singleton instance of this is available at
    :py:attr:`PayloadConverter.default`.
    """

    default_encoding_payload_converters: tuple[EncodingPayloadConverter, ...]
    """Default set of encoding payload converters the default payload converter
    uses.
    """

    def __init__(self) -> None:
        """Create a default payload converter."""
        super().__init__(*DefaultPayloadConverter.default_encoding_payload_converters)


class BinaryNullPayloadConverter(EncodingPayloadConverter):
    """Converter for 'binary/null' payloads supporting None values."""

    @property
    def encoding(self) -> str:
        """See base class."""
        return "binary/null"

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """See base class."""
        if value is None:
            return temporalio.api.common.v1.Payload(
                metadata={"encoding": self.encoding.encode()}
            )
        return None

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """See base class."""
        if len(payload.data) > 0:
            raise RuntimeError("Expected empty data set for binary/null")
        return None


class BinaryPlainPayloadConverter(EncodingPayloadConverter):
    """Converter for 'binary/plain' payloads supporting bytes values."""

    @property
    def encoding(self) -> str:
        """See base class."""
        return "binary/plain"

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """See base class."""
        if isinstance(value, bytes):
            return temporalio.api.common.v1.Payload(
                metadata={"encoding": self.encoding.encode()}, data=value
            )
        return None

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """See base class."""
        return payload.data


class JSONProtoPayloadConverter(EncodingPayloadConverter):
    """Converter for 'json/protobuf' payloads supporting protobuf Message values."""

    def __init__(self, ignore_unknown_fields: bool = False):
        """Initialize a JSON proto converter.

        Args:
            ignore_unknown_fields: Determines whether converter should error if
                unknown fields are detected
        """
        super().__init__()
        self._ignore_unknown_fields = ignore_unknown_fields

    @property
    def encoding(self) -> str:
        """See base class."""
        return "json/protobuf"

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """See base class."""
        if (
            isinstance(value, google.protobuf.message.Message)
            and value.DESCRIPTOR is not None  # type:ignore[reportUnnecessaryComparison]
        ):
            # We have to convert to dict then to JSON because MessageToJson does
            # not have a compact option removing spaces and newlines
            json_str = json.dumps(
                google.protobuf.json_format.MessageToDict(value),
                separators=(",", ":"),
                sort_keys=True,
            )
            return temporalio.api.common.v1.Payload(
                metadata={
                    "encoding": self.encoding.encode(),
                    "messageType": value.DESCRIPTOR.full_name.encode(),
                },
                data=json_str.encode(),
            )
        return None

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """See base class."""
        message_type = payload.metadata.get("messageType", b"<unknown>").decode()
        try:
            value = _sym_db.GetSymbol(message_type)()
            return google.protobuf.json_format.Parse(
                payload.data,
                value,
                ignore_unknown_fields=self._ignore_unknown_fields,
            )
        except KeyError as err:
            raise RuntimeError(f"Unknown Protobuf type {message_type}") from err
        except google.protobuf.json_format.ParseError as err:
            raise RuntimeError("Failed parsing") from err


class BinaryProtoPayloadConverter(EncodingPayloadConverter):
    """Converter for 'binary/protobuf' payloads supporting protobuf Message values."""

    @property
    def encoding(self) -> str:
        """See base class."""
        return "binary/protobuf"

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """See base class."""
        if (
            isinstance(value, google.protobuf.message.Message)
            and value.DESCRIPTOR is not None  # type:ignore[reportUnnecessaryComparison]
        ):
            return temporalio.api.common.v1.Payload(
                metadata={
                    "encoding": self.encoding.encode(),
                    "messageType": value.DESCRIPTOR.full_name.encode(),
                },
                data=value.SerializeToString(),
            )
        return None

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """See base class."""
        message_type = payload.metadata.get("messageType", b"<unknown>").decode()
        try:
            value = _sym_db.GetSymbol(message_type)()
            value.ParseFromString(payload.data)
            return value
        except KeyError as err:
            raise RuntimeError(f"Unknown Protobuf type {message_type}") from err
        except google.protobuf.message.DecodeError as err:
            raise RuntimeError("Failed parsing") from err


class _TemporalTransferTypePayloadConverter(PayloadConverter, WithSerializationContext):
    """Payload converter wrapper for registered Temporal transfer type converters.

    Values with a registered transfer type converter are first converted to their
    transfer type value, then encoded by the wrapped payload converter. When
    decoding to a type with a registered transfer type converter, the wrapped
    converter first decodes the payload to the transfer type value and this wrapper
    constructs the requested user-facing type from it.
    """

    _inner_payload_converter: PayloadConverter

    def __init__(self, inner_payload_converter: PayloadConverter) -> None:
        """Create a Temporal transfer type payload converter."""
        self._inner_payload_converter = inner_payload_converter

    @staticmethod
    def wrap(payload_converter: PayloadConverter) -> PayloadConverter:
        """Wrap a payload converter unless it is already wrapped."""
        if isinstance(payload_converter, _TemporalTransferTypePayloadConverter):
            return payload_converter
        return _TemporalTransferTypePayloadConverter(payload_converter)

    def to_payloads(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
        """See base class."""
        transfer_type_values: list[Any] = []
        for value in values:
            converter = _get_transfer_type_converter(type(value))
            if converter is not None:
                value = converter.to_transfer_type(value)
            transfer_type_values.append(value)
        return self._inner_payload_converter.to_payloads(transfer_type_values)

    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[Any]:
        """See base class."""
        if type_hints is None:
            return self._inner_payload_converter.from_payloads(payloads, None)
        converters = [
            _get_transfer_type_converter(type_hint) for type_hint in type_hints
        ]
        inner_type_hints = [
            converter.transfer_type if converter is not None else type_hint
            for converter, type_hint in zip(converters, type_hints)
        ]
        values = self._inner_payload_converter.from_payloads(
            payloads, typing.cast("list[type]", inner_type_hints)
        )
        return [
            converter.from_transfer_type(value, type_hint)
            if converter is not None
            else value
            for value, converter, type_hint in zip(values, converters, type_hints)
        ]

    def with_context(self, context: SerializationContext) -> Self:
        """Return a new instance with context set on the inner converter."""
        if not isinstance(self._inner_payload_converter, WithSerializationContext):
            return self
        inner_payload_converter = self._inner_payload_converter.with_context(context)
        if inner_payload_converter is self._inner_payload_converter:
            return self
        return type(self)(inner_payload_converter)


class AdvancedJSONEncoder(json.JSONEncoder):
    """Advanced JSON encoder.

    This encoder supports dataclasses and all iterables as lists.

    It also uses Pydantic v1's "dict" methods if available on the object,
    but this is deprecated. Pydantic users should upgrade to v2 and use
    temporalio.contrib.pydantic.pydantic_data_converter.
    """

    def default(self, o: Any) -> Any:
        """Override JSON encoding default.

        See :py:meth:`json.JSONEncoder.default`.
        """
        # Datetime support
        if isinstance(o, datetime):
            return o.isoformat()
        # Dataclass support
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        # Support for Pydantic v1's dict method
        dict_fn = getattr(o, "dict", None)
        if callable(dict_fn):
            return dict_fn()
        # Support for non-list iterables like set
        if not isinstance(o, list) and isinstance(o, collections.abc.Iterable):
            return list(o)
        # Support for UUID
        if isinstance(o, uuid.UUID):
            return str(o)
        return super().default(o)


JSONTypeConverterUnhandled = NewType("JSONTypeConverterUnhandled", object)
"""Type of :py:attr:`JSONTypeConverter.Unhandled`."""

_JSONTypeConverterUnhandled = JSONTypeConverterUnhandled


class JSONTypeConverter(ABC):
    """Converter for converting an object from Python :py:func:`json.loads`
    result (e.g. scalar, list, or dict) to a known type.
    """

    Unhandled: ClassVar[JSONTypeConverterUnhandled] = JSONTypeConverterUnhandled(
        object()
    )
    """Sentinel value that must be used as the result of
    :py:meth:`to_typed_value` to say the given type is not handled by this
    converter."""

    @abstractmethod
    def to_typed_value(
        self, hint: type, value: Any
    ) -> Any | None | JSONTypeConverterUnhandled:
        """Convert the given value to a type based on the given hint.

        Args:
            hint: Type hint to use to help in converting the value.
            value: Value as returned by :py:func:`json.loads`. Usually a scalar,
                list, or dict.

        Returns:
            The converted value or :py:attr:`Unhandled` if this converter does
            not handle this situation.
        """
        raise NotImplementedError


class JSONPlainPayloadConverter(EncodingPayloadConverter):
    """Converter for 'json/plain' payloads supporting common Python values.

    For encoding, this supports all values that :py:func:`json.dump` supports
    and by default adds extra encoding support for dataclasses, classes with
    ``dict()`` methods, and all iterables.

    For decoding, this uses type hints to attempt to rebuild the type from the
    type hint.
    """

    _encoder: type[json.JSONEncoder] | None
    _decoder: type[json.JSONDecoder] | None
    _encoding: str

    def __init__(
        self,
        *,
        encoder: type[json.JSONEncoder] | None = AdvancedJSONEncoder,
        decoder: type[json.JSONDecoder] | None = None,
        encoding: str = "json/plain",
        custom_type_converters: Sequence[JSONTypeConverter] = [],
    ) -> None:
        """Initialize a JSON data converter.

        Args:
            encoder: Custom encoder class object to use.
            decoder: Custom decoder class object to use.
            encoding: Encoding name to use.
            custom_type_converters: Set of custom type converters that are used
                when converting from a payload to type-hinted values.
        """
        super().__init__()
        self._encoder = encoder
        self._decoder = decoder
        self._encoding = encoding
        self._custom_type_converters = custom_type_converters

    @property
    def encoding(self) -> str:
        """See base class."""
        return self._encoding

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """See base class."""
        # Check for Pydantic v1
        if hasattr(value, "parse_obj"):
            warnings.warn(
                "If you're using Pydantic v2, use temporalio.contrib.pydantic.pydantic_data_converter. "
                "If you're using Pydantic v1 and cannot upgrade, refer to https://github.com/temporalio/samples-python/tree/main/pydantic_converter_v1 for better v1 support."
            )
        # We let JSON conversion errors be thrown to caller
        return temporalio.api.common.v1.Payload(
            metadata={"encoding": self._encoding.encode()},
            data=json.dumps(
                value, cls=self._encoder, separators=(",", ":"), sort_keys=True
            ).encode(),
        )

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """See base class."""
        try:
            obj = json.loads(payload.data, cls=self._decoder)
            if type_hint:
                obj = value_to_type(type_hint, obj, self._custom_type_converters)
            return obj
        except json.JSONDecodeError as err:
            raise RuntimeError("Failed parsing") from err


def _get_iso_datetime_parser() -> Callable[[str], datetime]:
    """Isolates system version check and returns relevant datetime passer

    Returns:
        A callable to parse date strings into datetimes.
    """
    if sys.version_info >= (3, 11):
        return datetime.fromisoformat  # type:ignore[reportUnreachable] # noqa
    else:
        # Isolate import for py > 3.11, as dependency only installed for < 3.11
        return parser.isoparse  # type:ignore[reportUnreachable]


def value_to_type(
    hint: type,
    value: Any,
    custom_converters: Sequence[JSONTypeConverter] = [],
) -> Any:
    """Convert a given value to the given type hint.

    This is used internally to convert a raw JSON loaded value to a specific
    type hint.

    Args:
        hint: Type hint to convert the value to.
        value: Raw value (e.g. primitive, dict, or list) to convert from.
        custom_converters: Set of custom converters to try before doing default
            conversion. Converters are tried in order and the first value that
            is not :py:attr:`JSONTypeConverter.Unhandled` will be returned from
            this function instead of doing default behavior.

    Returns:
        Converted value.

    Raises:
        TypeError: Unable to convert to the given hint.
    """
    # Try custom converters
    for conv in custom_converters:
        ret = conv.to_typed_value(hint, value)
        if ret is not JSONTypeConverter.Unhandled:
            return ret

    # Any or primitives
    if hint is Any:
        return value
    elif hint is datetime:
        if isinstance(value, str):
            try:
                return _get_iso_datetime_parser()(value)
            except ValueError as err:
                raise TypeError(f"Failed parsing datetime string: {value}") from err
        elif isinstance(value, datetime):
            return value
        raise TypeError(f"Expected datetime or ISO8601 string, got {type(value)}")
    elif hint is int or hint is float:
        if not isinstance(value, (int, float)):
            raise TypeError(f"Expected value to be int|float, was {type(value)}")
        return hint(value)
    elif hint is bool:
        if not isinstance(value, bool):
            raise TypeError(f"Expected value to be bool, was {type(value)}")
        return bool(value)
    elif hint is str:
        if not isinstance(value, str):
            raise TypeError(f"Expected value to be str, was {type(value)}")
        return str(value)
    elif hint is bytes:
        if not isinstance(value, (str, bytes, list)):
            raise TypeError(f"Expected value to be bytes, was {type(value)}")
        # In some other SDKs, this is serialized as a base64 string, but in
        # Python this is a numeric array.
        return bytes(value)  # type: ignore
    elif hint is type(None):
        if value is not None:
            raise TypeError(f"Expected None, got value of type {type(value)}")
        return None

    # NewType. Note we cannot simply check isinstance NewType here because it's
    # only been a class since 3.10. Instead we'll just check for the presence
    # of a supertype.
    supertype = getattr(hint, "__supertype__", None)
    if supertype:
        return value_to_type(supertype, value, custom_converters)

    # Load origin for other checks
    origin = getattr(hint, "__origin__", hint)
    type_args: tuple = getattr(hint, "__args__", ())

    # Literal
    if origin is Literal or origin is typing_extensions.Literal:
        if value not in type_args:
            raise TypeError(f"Value {value} not in literal values {type_args}")
        return value

    is_union = origin is typing.Union  # type:ignore[reportDeprecated]
    is_union = is_union or isinstance(origin, UnionType)

    # Union
    if is_union:
        # Try each one. Note, Optional is just a union w/ none.
        for arg in type_args:
            try:
                return value_to_type(arg, value, custom_converters)
            except Exception:
                pass
        raise TypeError(f"Failed converting to {hint} from {value}")

    # Mapping
    if inspect.isclass(origin) and issubclass(origin, collections.abc.Mapping):
        if not isinstance(value, collections.abc.Mapping):
            raise TypeError(f"Expected {hint}, value was {type(value)}")
        ret_dict = {}
        # If there are required or optional keys that means we are a TypedDict
        # and therefore can extract per-key types
        per_key_types: dict[str, type] | None = None
        if getattr(origin, "__required_keys__", None) or getattr(
            origin, "__optional_keys__", None
        ):
            per_key_types = get_type_hints(origin)
        key_type = (
            type_args[0]
            if len(type_args) > 0
            and type_args[0] is not Any
            and not isinstance(type_args[0], TypeVar)
            else None
        )
        value_type = (
            type_args[1]
            if len(type_args) > 1
            and type_args[1] is not Any
            and not isinstance(type_args[1], TypeVar)
            else None
        )
        # Convert each key/value
        for key, value in value.items():
            this_value_type = value_type
            if per_key_types:
                # TODO(cretz): Strict mode would fail an unknown key
                this_value_type = per_key_types.get(key)

            if key_type:
                # This function is used only by JSONPlainPayloadConverter. When
                # serializing to JSON, Python supports key types str, int, float, bool,
                # and None, serializing all to string representations. We now attempt to
                # use the provided type annotation to recover the original value with its
                # original type.
                try:
                    if isinstance(key, str):
                        if key_type is int or key_type is float:
                            key = key_type(key)
                        elif key_type is bool:
                            key = {"true": True, "false": False}[key]
                        elif key_type is type(None):
                            key = {"null": None}[key]

                    if not isinstance(key_type, type) or not isinstance(key, key_type):
                        key = value_to_type(key_type, key, custom_converters)
                except Exception as err:
                    raise TypeError(
                        f"Failed converting key {repr(key)} to type {key_type} in mapping {hint}"
                    ) from err

            if this_value_type:
                try:
                    value = value_to_type(this_value_type, value, custom_converters)
                except Exception as err:
                    raise TypeError(
                        f"Failed converting value for key {repr(key)} in mapping {hint}"
                    ) from err
            ret_dict[key] = value
        # If there are per-key types, it's a typed dict and we want to attempt
        # instantiation to get its validation
        if per_key_types:
            ret_dict = hint(**ret_dict)
        return ret_dict

    # Dataclass
    if dataclasses.is_dataclass(hint):
        if not isinstance(value, dict):
            raise TypeError(
                f"Cannot convert to dataclass {hint}, value is {type(value)} not dict"
            )
        # Obtain dataclass fields and check that all dict fields are there and
        # that no required fields are missing. Unknown fields are silently
        # ignored.
        fields = dataclasses.fields(hint)
        field_hints = get_type_hints(hint)
        field_values = {}
        for field in fields:
            field_value = value.get(field.name, dataclasses.MISSING)
            # We do not check whether field is required here. Rather, we let the
            # attempted instantiation of the dataclass raise if a field is
            # missing
            if field_value is not dataclasses.MISSING:
                try:
                    field_values[field.name] = value_to_type(
                        field_hints[field.name], field_value, custom_converters
                    )
                except Exception as err:
                    raise TypeError(
                        f"Failed converting field {field.name} on dataclass {hint}"
                    ) from err
        # Simply instantiate the dataclass. This will fail as expected when
        # missing required fields.
        # TODO(cretz): Want way to convert snake case to camel case?
        return hint(**field_values)

    # Pydantic model instance
    # Pydantic users should use Pydantic v2 with
    # temporalio.contrib.pydantic.pydantic_data_converter, in which case a
    # pydantic model instance will have been handled by the custom_converters at
    # the start of this function. We retain the following for backwards
    # compatibility with pydantic v1 users, but this is deprecated.
    parse_obj_attr = inspect.getattr_static(hint, "parse_obj", None)
    if isinstance(parse_obj_attr, classmethod) or isinstance(
        parse_obj_attr, staticmethod
    ):
        if not isinstance(value, dict):
            raise TypeError(
                f"Cannot convert to {hint}, value is {type(value)} not dict"
            )
        return getattr(hint, "parse_obj")(value)

    # IntEnum
    if inspect.isclass(hint) and issubclass(hint, IntEnum):
        if not isinstance(value, int):
            raise TypeError(
                f"Cannot convert to enum {hint}, value not an integer, value is {type(value)}"
            )
        return hint(value)

    # StrEnum, available in 3.11+
    if sys.version_info >= (3, 11):
        if inspect.isclass(hint) and issubclass(hint, StrEnum):  # type:ignore[reportUnreachable]
            if not isinstance(value, str):
                raise TypeError(
                    f"Cannot convert to enum {hint}, value not a string, value is {type(value)}"
                )
            return hint(value)

    # UUID
    if inspect.isclass(hint) and issubclass(hint, uuid.UUID):
        return hint(value)

    # Iterable. We intentionally put this last as it catches several others.
    if inspect.isclass(origin) and issubclass(origin, collections.abc.Iterable):
        if not isinstance(value, collections.abc.Iterable):
            raise TypeError(f"Expected {hint}, value was {type(value)}")
        ret_list = []
        # If there is no type arg, just return value as is
        if not type_args or (
            len(type_args) == 1
            and (isinstance(type_args[0], TypeVar) or type_args[0] is Ellipsis)
        ):
            ret_list = list(value)
        else:
            # Otherwise convert
            for i, item in enumerate(value):
                # Non-tuples use first type arg, tuples use arg set or one
                # before ellipsis if that's set
                if origin is not tuple:
                    arg_type = type_args[0]
                elif len(type_args) > i and type_args[i] is not Ellipsis:
                    arg_type = type_args[i]
                elif type_args[-1] is Ellipsis:
                    # Ellipsis means use the second to last one
                    arg_type = type_args[-2]  # type: ignore
                else:
                    raise TypeError(
                        f"Type {hint} only expecting {len(type_args)} values, got at least {i + 1}"
                    )
                try:
                    ret_list.append(value_to_type(arg_type, item, custom_converters))
                except Exception as err:
                    raise TypeError(f"Failed converting {hint} index {i}") from err
        # If tuple, set, or deque convert back to that type
        if origin is tuple:
            return tuple(ret_list)
        elif origin is set:
            return set(ret_list)
        elif origin is collections.deque:
            return collections.deque(ret_list)
        return ret_list

    raise TypeError(f"Unserializable type during conversion: {hint}")


# Set up after all converter classes are defined to avoid forward-reference issues.
DefaultPayloadConverter.default_encoding_payload_converters = (
    BinaryNullPayloadConverter(),
    BinaryPlainPayloadConverter(),
    JSONProtoPayloadConverter(),
    BinaryProtoPayloadConverter(),
    JSONPlainPayloadConverter(),  # JSON Plain needs to remain last because it throws on unknown types
)
