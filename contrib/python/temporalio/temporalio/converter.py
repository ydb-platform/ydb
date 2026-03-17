"""Base converter and implementations for data conversion."""

from __future__ import annotations

import collections
import collections.abc
import dataclasses
import inspect
import json
import sys
import traceback
import uuid
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from itertools import zip_longest
from logging import getLogger
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Mapping,
    NewType,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_type_hints,
    overload,
)

import google.protobuf.duration_pb2
import google.protobuf.json_format
import google.protobuf.message
import google.protobuf.symbol_database
import nexusrpc
import typing_extensions

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.failure.v1
import temporalio.api.sdk.v1
import temporalio.common
import temporalio.exceptions
import temporalio.types

if sys.version_info < (3, 11):
    # Python's datetime.fromisoformat doesn't support certain formats pre-3.11
    from dateutil import parser  # type: ignore
# StrEnum is available in 3.11+
if sys.version_info >= (3, 11):
    from enum import StrEnum

if sys.version_info >= (3, 10):
    from types import UnionType

logger = getLogger(__name__)


class PayloadConverter(ABC):
    """Base payload converter to/from multiple payloads/values."""

    default: ClassVar[PayloadConverter]
    """Default payload converter."""

    @abstractmethod
    def to_payloads(
        self, values: Sequence[Any]
    ) -> List[temporalio.api.common.v1.Payload]:
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
        type_hints: Optional[List[Type]] = None,
    ) -> List[Any]:
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
        self, payloads: Optional[temporalio.api.common.v1.Payloads]
    ) -> List[Any]:
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
        type_hint: Type[temporalio.types.AnyType],
    ) -> temporalio.types.AnyType: ...

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: Optional[Type] = None,
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
    def to_payload(self, value: Any) -> Optional[temporalio.api.common.v1.Payload]:
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
        type_hint: Optional[Type] = None,
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


class CompositePayloadConverter(PayloadConverter):
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
        # Insertion order preserved here since Python 3.7
        self.converters = {c.encoding.encode(): c for c in converters}

    def to_payloads(
        self, values: Sequence[Any]
    ) -> List[temporalio.api.common.v1.Payload]:
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
        type_hints: Optional[List[Type]] = None,
    ) -> List[Any]:
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


class DefaultPayloadConverter(CompositePayloadConverter):
    """Default payload converter compatible with other Temporal SDKs.

    This handles None, bytes, all protobuf message types, and any type that
    :py:func:`json.dump` accepts. A singleton instance of this is available at
    :py:attr:`PayloadConverter.default`.
    """

    default_encoding_payload_converters: Tuple[EncodingPayloadConverter, ...]
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

    def to_payload(self, value: Any) -> Optional[temporalio.api.common.v1.Payload]:
        """See base class."""
        if value is None:
            return temporalio.api.common.v1.Payload(
                metadata={"encoding": self.encoding.encode()}
            )
        return None

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: Optional[Type] = None,
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

    def to_payload(self, value: Any) -> Optional[temporalio.api.common.v1.Payload]:
        """See base class."""
        if isinstance(value, bytes):
            return temporalio.api.common.v1.Payload(
                metadata={"encoding": self.encoding.encode()}, data=value
            )
        return None

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: Optional[Type] = None,
    ) -> Any:
        """See base class."""
        return payload.data


_sym_db = google.protobuf.symbol_database.Default()


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

    def to_payload(self, value: Any) -> Optional[temporalio.api.common.v1.Payload]:
        """See base class."""
        if (
            isinstance(value, google.protobuf.message.Message)
            and value.DESCRIPTOR is not None
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
        type_hint: Optional[Type] = None,
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

    def to_payload(self, value: Any) -> Optional[temporalio.api.common.v1.Payload]:
        """See base class."""
        if (
            isinstance(value, google.protobuf.message.Message)
            and value.DESCRIPTOR is not None
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
        type_hint: Optional[Type] = None,
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


class JSONPlainPayloadConverter(EncodingPayloadConverter):
    """Converter for 'json/plain' payloads supporting common Python values.

    For encoding, this supports all values that :py:func:`json.dump` supports
    and by default adds extra encoding support for dataclasses, classes with
    ``dict()`` methods, and all iterables.

    For decoding, this uses type hints to attempt to rebuild the type from the
    type hint.
    """

    _encoder: Optional[Type[json.JSONEncoder]]
    _decoder: Optional[Type[json.JSONDecoder]]
    _encoding: str

    def __init__(
        self,
        *,
        encoder: Optional[Type[json.JSONEncoder]] = AdvancedJSONEncoder,
        decoder: Optional[Type[json.JSONDecoder]] = None,
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

    def to_payload(self, value: Any) -> Optional[temporalio.api.common.v1.Payload]:
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
        type_hint: Optional[Type] = None,
    ) -> Any:
        """See base class."""
        try:
            obj = json.loads(payload.data, cls=self._decoder)
            if type_hint:
                obj = value_to_type(type_hint, obj, self._custom_type_converters)
            return obj
        except json.JSONDecodeError as err:
            raise RuntimeError("Failed parsing") from err


_JSONTypeConverterUnhandled = NewType("_JSONTypeConverterUnhandled", object)


class JSONTypeConverter(ABC):
    """Converter for converting an object from Python :py:func:`json.loads`
    result (e.g. scalar, list, or dict) to a known type.
    """

    Unhandled = _JSONTypeConverterUnhandled(object())
    """Sentinel value that must be used as the result of
    :py:meth:`to_typed_value` to say the given type is not handled by this
    converter."""

    @abstractmethod
    def to_typed_value(
        self, hint: Type, value: Any
    ) -> Union[Optional[Any], _JSONTypeConverterUnhandled]:
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


class PayloadCodec(ABC):
    """Codec for encoding/decoding to/from bytes.

    Commonly used for compression or encryption.
    """

    @abstractmethod
    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> List[temporalio.api.common.v1.Payload]:
        """Encode the given payloads.

        Args:
            payloads: Payloads to encode. This value should not be mutated.

        Returns:
            Encoded payloads. Note, this does not have to be the same number as
            payloads given, but must be at least one and cannot be more than was
            given.
        """
        raise NotImplementedError

    @abstractmethod
    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> List[temporalio.api.common.v1.Payload]:
        """Decode the given payloads.

        Args:
            payloads: Payloads to decode. This value should not be mutated.

        Returns:
            Decoded payloads. Note, this does not have to be the same number as
            payloads given, but must be at least one and cannot be more than was
            given.
        """
        raise NotImplementedError

    async def encode_wrapper(self, payloads: temporalio.api.common.v1.Payloads) -> None:
        """:py:meth:`encode` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.

        This replaces the payloads within the wrapper.
        """
        new_payloads = await self.encode(payloads.payloads)
        del payloads.payloads[:]
        # TODO(cretz): Copy too expensive?
        payloads.payloads.extend(new_payloads)

    async def decode_wrapper(self, payloads: temporalio.api.common.v1.Payloads) -> None:
        """:py:meth:`decode` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.

        This replaces the payloads within.
        """
        new_payloads = await self.decode(payloads.payloads)
        del payloads.payloads[:]
        # TODO(cretz): Copy too expensive?
        payloads.payloads.extend(new_payloads)

    async def encode_failure(self, failure: temporalio.api.failure.v1.Failure) -> None:
        """Encode payloads of a failure."""
        await self._apply_to_failure_payloads(failure, self.encode_wrapper)

    async def decode_failure(self, failure: temporalio.api.failure.v1.Failure) -> None:
        """Decode payloads of a failure."""
        await self._apply_to_failure_payloads(failure, self.decode_wrapper)

    async def _apply_to_failure_payloads(
        self,
        failure: temporalio.api.failure.v1.Failure,
        cb: Callable[[temporalio.api.common.v1.Payloads], Awaitable[None]],
    ) -> None:
        if failure.HasField("encoded_attributes"):
            # Wrap in payloads and merge back
            payloads = temporalio.api.common.v1.Payloads(
                payloads=[failure.encoded_attributes]
            )
            await cb(payloads)
            failure.encoded_attributes.CopyFrom(payloads.payloads[0])
        if failure.HasField(
            "application_failure_info"
        ) and failure.application_failure_info.HasField("details"):
            await cb(failure.application_failure_info.details)
        elif failure.HasField(
            "timeout_failure_info"
        ) and failure.timeout_failure_info.HasField("last_heartbeat_details"):
            await cb(failure.timeout_failure_info.last_heartbeat_details)
        elif failure.HasField(
            "canceled_failure_info"
        ) and failure.canceled_failure_info.HasField("details"):
            await cb(failure.canceled_failure_info.details)
        elif failure.HasField(
            "reset_workflow_failure_info"
        ) and failure.reset_workflow_failure_info.HasField("last_heartbeat_details"):
            await cb(failure.reset_workflow_failure_info.last_heartbeat_details)
        if failure.HasField("cause"):
            await self._apply_to_failure_payloads(failure.cause, cb)


class FailureConverter(ABC):
    """Base failure converter to/from errors.

    Note, for workflow exceptions, :py:attr:`to_failure` is only invoked if the
    exception is an instance of :py:class:`temporalio.exceptions.FailureError`.
    Users should extend :py:class:`temporalio.exceptions.ApplicationError` if
    they want a custom workflow exception to work with this class.
    """

    default: ClassVar[FailureConverter]
    """Default failure converter."""

    @abstractmethod
    def to_failure(
        self,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        """Convert the given exception to a Temporal failure.

        Users should make sure not to alter the ``exception`` input.

        Args:
            exception: The exception to convert.
            payload_converter: The payload converter to use if needed.
            failure: The failure to update with error information.
        """
        raise NotImplementedError

    @abstractmethod
    def from_failure(
        self,
        failure: temporalio.api.failure.v1.Failure,
        payload_converter: PayloadConverter,
    ) -> BaseException:
        """Convert the given Temporal failure to an exception.

        Users should make sure not to alter the ``failure`` input.

        Args:
            failure: The failure to convert.
            payload_converter: The payload converter to use if needed.

        Returns:
            Converted error.
        """
        raise NotImplementedError


class DefaultFailureConverter(FailureConverter):
    """Default failure converter.

    A singleton instance of this is available at
    :py:attr:`FailureConverter.default`.
    """

    def __init__(self, *, encode_common_attributes: bool = False) -> None:
        """Create the default failure converter.

        Args:
            encode_common_attributes: If ``True``, the message and stack trace
                of the failure will be moved into the encoded attribute section
                of the failure which can be encoded with a codec.
        """
        super().__init__()
        self._encode_common_attributes = encode_common_attributes

    def to_failure(
        self,
        exception: BaseException,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        """See base class."""
        # If already a failure error, use that
        if isinstance(exception, temporalio.exceptions.FailureError):
            self._error_to_failure(exception, payload_converter, failure)
        elif isinstance(exception, nexusrpc.HandlerError):
            self._nexus_handler_error_to_failure(exception, payload_converter, failure)
        else:
            # Convert to failure error
            failure_error = temporalio.exceptions.ApplicationError(
                str(exception), type=exception.__class__.__name__
            )
            failure_error.__traceback__ = exception.__traceback__
            failure_error.__cause__ = exception.__cause__
            self._error_to_failure(failure_error, payload_converter, failure)
        # Encode common attributes if requested
        if self._encode_common_attributes:
            # Move message and stack trace to encoded attribute payload
            failure.encoded_attributes.CopyFrom(
                payload_converter.to_payloads(
                    [{"message": failure.message, "stack_trace": failure.stack_trace}]
                )[0]
            )
            failure.message = "Encoded failure"
            failure.stack_trace = ""

    def _error_to_failure(
        self,
        error: temporalio.exceptions.FailureError,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        # If there is an underlying proto already, just use that
        if error.failure:
            failure.CopyFrom(error.failure)
            return

        # Set message, stack, and cause. Obtaining cause follows rules from
        # https://docs.python.org/3/library/exceptions.html#exception-context
        failure.message = error.message
        if error.__traceback__:
            failure.stack_trace = "\n".join(traceback.format_tb(error.__traceback__))
        if error.__cause__:
            self.to_failure(error.__cause__, payload_converter, failure.cause)
        elif not error.__suppress_context__ and error.__context__:
            self.to_failure(error.__context__, payload_converter, failure.cause)

        # Set specific subclass values
        if isinstance(error, temporalio.exceptions.ApplicationError):
            failure.application_failure_info.SetInParent()
            if error.type:
                failure.application_failure_info.type = error.type
            failure.application_failure_info.non_retryable = error.non_retryable
            if error.details:
                failure.application_failure_info.details.CopyFrom(
                    payload_converter.to_payloads_wrapper(error.details)
                )
            if error.next_retry_delay:
                failure.application_failure_info.next_retry_delay.FromTimedelta(
                    error.next_retry_delay
                )
            if error.category:
                failure.application_failure_info.category = (
                    temporalio.api.enums.v1.ApplicationErrorCategory.ValueType(
                        error.category
                    )
                )
        elif isinstance(error, temporalio.exceptions.TimeoutError):
            failure.timeout_failure_info.SetInParent()
            failure.timeout_failure_info.timeout_type = (
                temporalio.api.enums.v1.TimeoutType.ValueType(error.type or 0)
            )
            if error.last_heartbeat_details:
                failure.timeout_failure_info.last_heartbeat_details.CopyFrom(
                    payload_converter.to_payloads_wrapper(error.last_heartbeat_details)
                )
        elif isinstance(error, temporalio.exceptions.CancelledError):
            failure.canceled_failure_info.SetInParent()
            if error.details:
                failure.canceled_failure_info.details.CopyFrom(
                    payload_converter.to_payloads_wrapper(error.details)
                )
        elif isinstance(error, temporalio.exceptions.TerminatedError):
            failure.terminated_failure_info.SetInParent()
        elif isinstance(error, temporalio.exceptions.ServerError):
            failure.server_failure_info.SetInParent()
            failure.server_failure_info.non_retryable = error.non_retryable
        elif isinstance(error, temporalio.exceptions.ActivityError):
            failure.activity_failure_info.SetInParent()
            failure.activity_failure_info.scheduled_event_id = error.scheduled_event_id
            failure.activity_failure_info.started_event_id = error.started_event_id
            failure.activity_failure_info.identity = error.identity
            failure.activity_failure_info.activity_type.name = error.activity_type
            failure.activity_failure_info.activity_id = error.activity_id
            failure.activity_failure_info.retry_state = (
                temporalio.api.enums.v1.RetryState.ValueType(error.retry_state or 0)
            )
        elif isinstance(error, temporalio.exceptions.ChildWorkflowError):
            failure.child_workflow_execution_failure_info.SetInParent()
            failure.child_workflow_execution_failure_info.namespace = error.namespace
            failure.child_workflow_execution_failure_info.workflow_execution.workflow_id = error.workflow_id
            failure.child_workflow_execution_failure_info.workflow_execution.run_id = (
                error.run_id
            )
            failure.child_workflow_execution_failure_info.workflow_type.name = (
                error.workflow_type
            )
            failure.child_workflow_execution_failure_info.initiated_event_id = (
                error.initiated_event_id
            )
            failure.child_workflow_execution_failure_info.started_event_id = (
                error.started_event_id
            )
            failure.child_workflow_execution_failure_info.retry_state = (
                temporalio.api.enums.v1.RetryState.ValueType(error.retry_state or 0)
            )
        # TODO(nexus-preview): missing test coverage
        elif isinstance(error, temporalio.exceptions.NexusOperationError):
            failure.nexus_operation_execution_failure_info.SetInParent()
            failure.nexus_operation_execution_failure_info.scheduled_event_id = (
                error.scheduled_event_id
            )
            failure.nexus_operation_execution_failure_info.endpoint = error.endpoint
            failure.nexus_operation_execution_failure_info.service = error.service
            failure.nexus_operation_execution_failure_info.operation = error.operation
            failure.nexus_operation_execution_failure_info.operation_token = (
                error.operation_token
            )

    def _nexus_handler_error_to_failure(
        self,
        error: nexusrpc.HandlerError,
        payload_converter: PayloadConverter,
        failure: temporalio.api.failure.v1.Failure,
    ) -> None:
        failure.message = str(error)
        if error.__traceback__:
            failure.stack_trace = "\n".join(traceback.format_tb(error.__traceback__))
        if error.__cause__:
            self.to_failure(error.__cause__, payload_converter, failure.cause)
        failure.nexus_handler_failure_info.SetInParent()
        failure.nexus_handler_failure_info.type = error.type.name
        failure.nexus_handler_failure_info.retry_behavior = temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.ValueType(
            temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
            if error.retryable_override is True
            else temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
            if error.retryable_override is False
            else temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
        )

    def from_failure(
        self,
        failure: temporalio.api.failure.v1.Failure,
        payload_converter: PayloadConverter,
    ) -> BaseException:
        """See base class."""
        # If encoded attributes are present and have the fields we expect,
        # extract them
        if failure.HasField("encoded_attributes"):
            # Clone the failure to not mutate the incoming failure
            new_failure = temporalio.api.failure.v1.Failure()
            new_failure.CopyFrom(failure)
            failure = new_failure
            try:
                encoded_attributes: Dict[str, Any] = payload_converter.from_payloads(
                    [failure.encoded_attributes]
                )[0]
                if isinstance(encoded_attributes, dict):
                    message = encoded_attributes.get("message")
                    if isinstance(message, str):
                        failure.message = message
                    stack_trace = encoded_attributes.get("stack_trace")
                    if isinstance(stack_trace, str):
                        failure.stack_trace = stack_trace
            except:
                pass

        err: Union[temporalio.exceptions.FailureError, nexusrpc.HandlerError]
        if failure.HasField("application_failure_info"):
            app_info = failure.application_failure_info
            err = temporalio.exceptions.ApplicationError(
                failure.message or "Application error",
                *payload_converter.from_payloads_wrapper(app_info.details),
                type=app_info.type or None,
                non_retryable=app_info.non_retryable,
                next_retry_delay=app_info.next_retry_delay.ToTimedelta(),
                category=temporalio.exceptions.ApplicationErrorCategory(
                    int(app_info.category)
                ),
            )
        elif failure.HasField("timeout_failure_info"):
            timeout_info = failure.timeout_failure_info
            err = temporalio.exceptions.TimeoutError(
                failure.message or "Timeout",
                type=temporalio.exceptions.TimeoutType(int(timeout_info.timeout_type))
                if timeout_info.timeout_type
                else None,
                last_heartbeat_details=payload_converter.from_payloads_wrapper(
                    timeout_info.last_heartbeat_details
                ),
            )
        elif failure.HasField("canceled_failure_info"):
            cancel_info = failure.canceled_failure_info
            err = temporalio.exceptions.CancelledError(
                failure.message or "Cancelled",
                *payload_converter.from_payloads_wrapper(cancel_info.details),
            )
        elif failure.HasField("terminated_failure_info"):
            err = temporalio.exceptions.TerminatedError(failure.message or "Terminated")
        elif failure.HasField("server_failure_info"):
            server_info = failure.server_failure_info
            err = temporalio.exceptions.ServerError(
                failure.message or "Server error",
                non_retryable=server_info.non_retryable,
            )
        elif failure.HasField("activity_failure_info"):
            act_info = failure.activity_failure_info
            err = temporalio.exceptions.ActivityError(
                failure.message or "Activity error",
                scheduled_event_id=act_info.scheduled_event_id,
                started_event_id=act_info.started_event_id,
                identity=act_info.identity,
                activity_type=act_info.activity_type.name,
                activity_id=act_info.activity_id,
                retry_state=temporalio.exceptions.RetryState(int(act_info.retry_state))
                if act_info.retry_state
                else None,
            )
        elif failure.HasField("child_workflow_execution_failure_info"):
            child_info = failure.child_workflow_execution_failure_info
            err = temporalio.exceptions.ChildWorkflowError(
                failure.message or "Child workflow error",
                namespace=child_info.namespace,
                workflow_id=child_info.workflow_execution.workflow_id,
                run_id=child_info.workflow_execution.run_id,
                workflow_type=child_info.workflow_type.name,
                initiated_event_id=child_info.initiated_event_id,
                started_event_id=child_info.started_event_id,
                retry_state=temporalio.exceptions.RetryState(
                    int(child_info.retry_state)
                )
                if child_info.retry_state
                else None,
            )
        elif failure.HasField("nexus_handler_failure_info"):
            nexus_handler_failure_info = failure.nexus_handler_failure_info
            try:
                _type = nexusrpc.HandlerErrorType[nexus_handler_failure_info.type]
            except KeyError:
                logger.warning(
                    f"Unknown Nexus HandlerErrorType: {nexus_handler_failure_info.type}"
                )
                _type = nexusrpc.HandlerErrorType.INTERNAL
            retryable_override = (
                True
                if (
                    nexus_handler_failure_info.retry_behavior
                    == temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
                )
                else False
                if (
                    nexus_handler_failure_info.retry_behavior
                    == temporalio.api.enums.v1.NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
                )
                else None
            )
            err = nexusrpc.HandlerError(
                failure.message or "Nexus handler error",
                type=_type,
                retryable_override=retryable_override,
            )
        elif failure.HasField("nexus_operation_execution_failure_info"):
            nexus_op_failure_info = failure.nexus_operation_execution_failure_info
            err = temporalio.exceptions.NexusOperationError(
                failure.message or "Nexus operation error",
                scheduled_event_id=nexus_op_failure_info.scheduled_event_id,
                endpoint=nexus_op_failure_info.endpoint,
                service=nexus_op_failure_info.service,
                operation=nexus_op_failure_info.operation,
                operation_token=nexus_op_failure_info.operation_token,
            )
        else:
            err = temporalio.exceptions.FailureError(failure.message or "Failure error")
        if isinstance(err, temporalio.exceptions.FailureError):
            err._failure = failure
        if failure.HasField("cause"):
            err.__cause__ = self.from_failure(failure.cause, payload_converter)
        return err


class DefaultFailureConverterWithEncodedAttributes(DefaultFailureConverter):
    """Implementation of :py:class:`DefaultFailureConverter` which moves message
    and stack trace to encoded attributes subject to a codec.
    """

    def __init__(self) -> None:
        """Create a default failure converter with encoded attributes."""
        super().__init__(encode_common_attributes=True)


@dataclass(frozen=True)
class DataConverter:
    """Data converter for converting and encoding payloads to/from Python values.

    This combines :py:class:`PayloadConverter` which converts values with
    :py:class:`PayloadCodec` which encodes bytes.
    """

    payload_converter_class: Type[PayloadConverter] = DefaultPayloadConverter
    """Class to instantiate for payload conversion."""

    payload_codec: Optional[PayloadCodec] = None
    """Optional codec for encoding payload bytes."""

    failure_converter_class: Type[FailureConverter] = DefaultFailureConverter
    """Class to instantiate for failure conversion."""

    payload_converter: PayloadConverter = dataclasses.field(init=False)
    """Payload converter created from the :py:attr:`payload_converter_class`."""

    failure_converter: FailureConverter = dataclasses.field(init=False)
    """Failure converter created from the :py:attr:`failure_converter_class`."""

    default: ClassVar[DataConverter]
    """Singleton default data converter."""

    def __post_init__(self) -> None:  # noqa: D105
        object.__setattr__(self, "payload_converter", self.payload_converter_class())
        object.__setattr__(self, "failure_converter", self.failure_converter_class())

    async def encode(
        self, values: Sequence[Any]
    ) -> List[temporalio.api.common.v1.Payload]:
        """Encode values into payloads.

        First converts values to payloads then encodes payloads using codec.

        Args:
            values: Values to be converted and encoded.

        Returns:
            Converted and encoded payloads. Note, this does not have to be the
            same number as values given, but must be at least one and cannot be
            more than was given.
        """
        payloads = self.payload_converter.to_payloads(values)
        if self.payload_codec:
            payloads = await self.payload_codec.encode(payloads)
        return payloads

    async def decode(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: Optional[List[Type]] = None,
    ) -> List[Any]:
        """Decode payloads into values.

        First decodes payloads using codec then converts payloads to values.

        Args:
            payloads: Payloads to be decoded and converted.

        Returns:
            Decoded and converted values.
        """
        if self.payload_codec:
            payloads = await self.payload_codec.decode(payloads)
        return self.payload_converter.from_payloads(payloads, type_hints)

    async def encode_wrapper(
        self, values: Sequence[Any]
    ) -> temporalio.api.common.v1.Payloads:
        """:py:meth:`encode` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.
        """
        return temporalio.api.common.v1.Payloads(payloads=(await self.encode(values)))

    async def decode_wrapper(
        self,
        payloads: Optional[temporalio.api.common.v1.Payloads],
        type_hints: Optional[List[Type]] = None,
    ) -> List[Any]:
        """:py:meth:`decode` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.
        """
        if not payloads or not payloads.payloads:
            return []
        return await self.decode(payloads.payloads, type_hints)

    async def encode_failure(
        self, exception: BaseException, failure: temporalio.api.failure.v1.Failure
    ) -> None:
        """Convert and encode failure."""
        self.failure_converter.to_failure(exception, self.payload_converter, failure)
        if self.payload_codec:
            await self.payload_codec.encode_failure(failure)

    async def decode_failure(
        self, failure: temporalio.api.failure.v1.Failure
    ) -> BaseException:
        """Decode and convert failure."""
        if self.payload_codec:
            await self.payload_codec.decode_failure(failure)
        return self.failure_converter.from_failure(failure, self.payload_converter)


DefaultPayloadConverter.default_encoding_payload_converters = (
    BinaryNullPayloadConverter(),
    BinaryPlainPayloadConverter(),
    JSONProtoPayloadConverter(),
    BinaryProtoPayloadConverter(),
    JSONPlainPayloadConverter(),  # JSON Plain needs to remain in last because it throws on unknown types
)

DataConverter.default = DataConverter()

PayloadConverter.default = DataConverter.default.payload_converter

FailureConverter.default = DataConverter.default.failure_converter


def default() -> DataConverter:
    """Default data converter.

    .. deprecated::
        Use :py:meth:`DataConverter.default` instead.
    """
    return DataConverter.default


def encode_search_attributes(
    attributes: Union[
        temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
    ],
    api: temporalio.api.common.v1.SearchAttributes,
) -> None:
    """Convert search attributes into an API message.

    Args:
        attributes: Search attributes to convert. The dictionary form of this is
            DEPRECATED.
        api: API message to set converted attributes on.
    """
    if isinstance(attributes, temporalio.common.TypedSearchAttributes):
        for typed_k, typed_v in attributes:
            api.indexed_fields[typed_k.name].CopyFrom(
                encode_typed_search_attribute_value(typed_k, typed_v)
            )
        return
    elif not attributes:
        return
    for k, v in attributes.items():
        api.indexed_fields[k].CopyFrom(encode_search_attribute_values(v))


def encode_typed_search_attribute_value(
    key: temporalio.common.SearchAttributeKey[
        temporalio.common.SearchAttributeValueType
    ],
    value: Optional[temporalio.common.SearchAttributeValue],
) -> temporalio.api.common.v1.Payload:
    """Convert typed search attribute value into a payload.

    Args:
        key: Key for the value.
        value: Value to convert.

    Returns:
        Payload for the value.
    """
    # For server search attributes to work properly, we cannot set the metadata
    # type when we set null
    if value is None:
        return default().payload_converter.to_payload(None)
    if not isinstance(value, key.origin_value_type):
        raise TypeError(
            f"Value of type {value} not suitable for indexed value type {key.indexed_value_type}"
        )
    # datetime needs to be in isoformat
    if isinstance(value, datetime):
        value = value.isoformat()
    # We'll do an extra sanity check for keyword list and check every value
    if isinstance(value, Sequence):
        for v in value:
            if not isinstance(v, str):
                raise TypeError("All values of a keyword list must be strings")
    # Convert value
    payload = default().payload_converter.to_payload(value)
    # Set metadata type
    payload.metadata["type"] = key._metadata_type.encode()
    return payload


def encode_search_attribute_values(
    vals: temporalio.common.SearchAttributeValues,
) -> temporalio.api.common.v1.Payload:
    """Convert search attribute values into a payload.

    .. deprecated::
        Use typed search attributes instead.

    Args:
        vals: List of values to convert.
    """
    if not isinstance(vals, list):
        raise TypeError("Search attribute values must be lists")
    # Confirm all types are the same
    val_type: Optional[Type] = None
    # Convert dates to strings
    safe_vals = []
    for v in vals:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                raise ValueError(
                    "Timezone must be present on all search attribute dates"
                )
            v = v.isoformat()
        elif not isinstance(v, (str, int, float, bool)):
            raise TypeError(
                f"Search attribute value of type {type(v).__name__} not one of str, int, float, bool, or datetime"
            )
        elif val_type and type(v) is not val_type:
            raise TypeError(
                "Search attribute values must have the same type for the same key"
            )
        elif not val_type:
            val_type = type(v)
        safe_vals.append(v)
    return default().payload_converter.to_payloads([safe_vals])[0]


def _encode_maybe_typed_search_attributes(
    non_typed_attributes: Optional[temporalio.common.SearchAttributes],
    typed_attributes: Optional[temporalio.common.TypedSearchAttributes],
    api: temporalio.api.common.v1.SearchAttributes,
) -> None:
    if non_typed_attributes:
        if typed_attributes and typed_attributes.search_attributes:
            raise ValueError(
                "Cannot provide both deprecated search attributes and typed search attributes"
            )
        encode_search_attributes(non_typed_attributes, api)
    elif typed_attributes and typed_attributes.search_attributes:
        encode_search_attributes(typed_attributes, api)


def _get_iso_datetime_parser() -> Callable[[str], datetime]:
    """Isolates system version check and returns relevant datetime passer

    Returns:
        A callable to parse date strings into datetimes.
    """
    if sys.version_info >= (3, 11):
        return datetime.fromisoformat  # noqa
    else:
        # Isolate import for py > 3.11, as dependency only installed for < 3.11
        return parser.isoparse


def decode_search_attributes(
    api: temporalio.api.common.v1.SearchAttributes,
) -> temporalio.common.SearchAttributes:
    """Decode API search attributes to values.

    .. deprecated::
        Use typed search attributes instead.

    Args:
        api: API message with search attribute values to convert.

    Returns:
        Converted search attribute values (new mapping every time).
    """
    conv = default().payload_converter
    ret = {}
    for k, v in api.indexed_fields.items():
        val = conv.from_payloads([v])[0]
        # If a value did not come back as a list, make it a single-item list
        if not isinstance(val, list):
            val = [val]
        # Convert each item to datetime if necessary
        if v.metadata.get("type") == b"Datetime":
            parser = _get_iso_datetime_parser()
            val = [parser(v) for v in val]
        ret[k] = val
    return ret


def decode_typed_search_attributes(
    api: temporalio.api.common.v1.SearchAttributes,
) -> temporalio.common.TypedSearchAttributes:
    """Decode API search attributes to typed search attributes.

    Args:
        api: API message with search attribute values to convert.

    Returns:
        Typed search attribute collection (new object every time).
    """
    conv = default().payload_converter
    pairs: List[temporalio.common.SearchAttributePair] = []
    for k, v in api.indexed_fields.items():
        # We want the "type" metadata, but if it is not present or an unknown
        # type, we will just ignore
        metadata_type = v.metadata.get("type")
        if not metadata_type:
            continue
        key = temporalio.common.SearchAttributeKey._from_metadata_type(
            k, metadata_type.decode()
        )
        if not key:
            continue
        val = conv.from_payload(v)
        # If the value is a list but the type is not keyword list, pull out
        # single item or consider this an invalid value and ignore
        if (
            key.indexed_value_type
            != temporalio.common.SearchAttributeIndexedValueType.KEYWORD_LIST
            and isinstance(val, list)
        ):
            if len(val) != 1:
                continue
            val = val[0]
        if (
            key.indexed_value_type
            == temporalio.common.SearchAttributeIndexedValueType.DATETIME
        ):
            parser = _get_iso_datetime_parser()
            # We will let this throw
            val = parser(val)
        # If the value isn't the right type, we need to ignore
        if isinstance(val, key.origin_value_type):
            pairs.append(temporalio.common.SearchAttributePair(key, val))
    return temporalio.common.TypedSearchAttributes(pairs)


def _decode_search_attribute_value(
    payload: temporalio.api.common.v1.Payload,
) -> temporalio.common.SearchAttributeValue:
    val = default().payload_converter.from_payload(payload)
    if isinstance(val, str) and payload.metadata.get("type") == b"Datetime":
        val = _get_iso_datetime_parser()(val)
    return val  # type: ignore


def value_to_type(
    hint: Type,
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
    type_args: Tuple = getattr(hint, "__args__", ())

    # Literal
    if origin is Literal or origin is typing_extensions.Literal:
        if value not in type_args:
            raise TypeError(f"Value {value} not in literal values {type_args}")
        return value

    is_union = origin is Union
    if sys.version_info >= (3, 10):
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
        per_key_types: Optional[Dict[str, Type]] = None
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

                    if not isinstance(key, key_type):
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
        if inspect.isclass(hint) and issubclass(hint, StrEnum):
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
