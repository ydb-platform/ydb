"""DataConverter: the top-level data conversion orchestrator."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import Self

import temporalio.api.common.v1
import temporalio.api.failure.v1
import temporalio.common
from temporalio.converter._extstore import (
    _REFERENCE_ENCODING,
    _REFERENCE_MESSAGE_TYPE,
    ExternalStorage,
    StorageDriverStoreContext,
)
from temporalio.converter._failure_converter import (
    FailureConverter,
)
from temporalio.converter._payload_codec import (
    PayloadCodec,
    _apply_to_failure_payloads,
)
from temporalio.converter._payload_converter import (
    PayloadConverter,
    _TemporalTransferTypePayloadConverter,
)
from temporalio.converter._serialization_context import (
    SerializationContext,
    WithSerializationContext,
)


def _is_reference_payload(p: temporalio.api.common.v1.Payload) -> bool:
    """Return True if *p* is an external-storage reference payload."""
    return p.metadata.get("encoding") == _REFERENCE_ENCODING or (
        p.metadata.get("encoding") == b"json/protobuf"
        and p.metadata.get("messageType") == _REFERENCE_MESSAGE_TYPE
    )


# Import defaults from public API to avoid pydoctor cross-reference issues
if TYPE_CHECKING:
    from temporalio.converter import DefaultFailureConverter, DefaultPayloadConverter
else:
    # Import from private modules for runtime to avoid circular imports
    from temporalio.converter._failure_converter import DefaultFailureConverter
    from temporalio.converter._payload_converter import DefaultPayloadConverter

logger = getLogger("temporalio.converter")


@dataclass(frozen=True)
class DataConverter(WithSerializationContext):
    """Data converter for converting and encoding payloads to/from Python values.

    This combines :py:class:`PayloadConverter` which converts values with
    :py:class:`PayloadCodec` which encodes bytes.
    """

    payload_converter_class: type[PayloadConverter] = DefaultPayloadConverter
    """Class to instantiate for payload conversion."""

    payload_codec: PayloadCodec | None = None
    """Optional codec for encoding payload bytes."""

    failure_converter_class: type[FailureConverter] = DefaultFailureConverter
    """Class to instantiate for failure conversion."""

    payload_converter: PayloadConverter = dataclasses.field(init=False)
    """Payload converter created from the :py:attr:`payload_converter_class`."""

    failure_converter: FailureConverter = dataclasses.field(init=False)
    """Failure converter created from the :py:attr:`failure_converter_class`."""

    external_storage: ExternalStorage | None = None
    """Options for external storage. If None, external storage is disabled.
        
    .. warning::
        This API is experimental.
    """

    default: ClassVar[DataConverter]
    """Singleton default data converter."""

    def __post_init__(self) -> None:  # noqa: D105
        object.__setattr__(self, "payload_converter", self._new_payload_converter())
        object.__setattr__(self, "failure_converter", self.failure_converter_class())

    def _new_payload_converter(self) -> PayloadConverter:
        """Create a payload converter instance with SDK transfer type hooks enabled."""
        return _TemporalTransferTypePayloadConverter.wrap(
            self.payload_converter_class()
        )

    async def encode(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
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
        payloads = await self._encode_payload_sequence(payloads)
        payloads = await self._external_store_payload_sequence(payloads)
        return payloads

    async def decode(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[Any]:
        """Decode payloads into values.

        First decodes payloads using codec then converts payloads to values.

        Args:
            payloads: Payloads to be decoded and converted.

        Returns:
            Decoded and converted values.
        """
        payloads = await self._external_retrieve_payload_sequence(payloads)
        payloads = await self._decode_payload_sequence(payloads)
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
        payloads: temporalio.api.common.v1.Payloads | None,
        type_hints: list[type] | None = None,
    ) -> list[Any]:
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
        await _apply_to_failure_payloads(failure, self._transform_outbound_payloads)

    async def decode_failure(
        self, failure: temporalio.api.failure.v1.Failure
    ) -> BaseException:
        """Decode and convert failure."""
        await _apply_to_failure_payloads(failure, self._transform_inbound_payloads)
        return self.failure_converter.from_failure(failure, self.payload_converter)

    def with_context(self, context: SerializationContext) -> Self:
        """Return an instance with context set on the component converters."""
        payload_converter = self.payload_converter
        payload_codec = self.payload_codec
        failure_converter = self.failure_converter
        external_storage = self.external_storage
        if isinstance(payload_converter, WithSerializationContext):
            payload_converter = payload_converter.with_context(context)
        if isinstance(payload_codec, WithSerializationContext):
            payload_codec = payload_codec.with_context(context)
        if isinstance(failure_converter, WithSerializationContext):
            failure_converter = failure_converter.with_context(context)
        if isinstance(external_storage, WithSerializationContext):
            external_storage = external_storage.with_context(context)
        if all(
            new is orig
            for new, orig in [
                (payload_converter, self.payload_converter),
                (payload_codec, self.payload_codec),
                (failure_converter, self.failure_converter),
                (external_storage, self.external_storage),
            ]
        ):
            return self
        cloned = dataclasses.replace(self)
        object.__setattr__(cloned, "payload_converter", payload_converter)
        object.__setattr__(cloned, "payload_codec", payload_codec)
        object.__setattr__(cloned, "failure_converter", failure_converter)
        object.__setattr__(cloned, "external_storage", external_storage)
        return cloned

    def _with_store_context(
        self, store_ctx: StorageDriverStoreContext
    ) -> DataConverter:
        """Return an instance with ``store_ctx`` bound into :attr:`external_storage`."""
        if self.external_storage is None:
            return self
        return dataclasses.replace(
            self,
            external_storage=self.external_storage._with_store_context(store_ctx),
        )

    def _with_contexts(
        self,
        serialization_ctx: SerializationContext,
        store_ctx: StorageDriverStoreContext,
    ) -> DataConverter:
        """Return an instance with both serialization and store contexts applied."""
        return self.with_context(serialization_ctx)._with_store_context(store_ctx)

    async def _decode_memo(
        self,
        source: temporalio.api.common.v1.Memo,
    ) -> Mapping[str, Any]:
        mapping: dict[str, Any] = {}
        for k, v in source.fields.items():
            mapping[k] = (await self.decode([v]))[0]
        return mapping

    async def _decode_memo_field(
        self,
        source: temporalio.api.common.v1.Memo,
        key: str,
        default: Any,
        type_hint: type | None,
    ) -> dict[str, Any]:
        payload = source.fields.get(key)
        if not payload:
            if default is temporalio.common._arg_unset:
                raise KeyError(f"Memo does not have a value for key {key}")
            return default
        return (await self.decode([payload], [type_hint] if type_hint else None))[0]

    async def _encode_memo(
        self, source: Mapping[str, Any]
    ) -> temporalio.api.common.v1.Memo:
        memo = temporalio.api.common.v1.Memo()
        await self._encode_memo_existing(source, memo)
        return memo

    async def _encode_memo_existing(
        self, source: Mapping[str, Any], memo: temporalio.api.common.v1.Memo
    ):
        for k, v in source.items():
            payload = v
            if not isinstance(v, temporalio.api.common.v1.Payload):
                payload = (await self.encode([v]))[0]
            memo.fields[k].CopyFrom(payload)

    async def _transform_outbound_payload(
        self, payload: temporalio.api.common.v1.Payload
    ) -> temporalio.api.common.v1.Payload:
        if self.payload_codec:
            payload = (await self.payload_codec.encode([payload]))[0]
        if self.external_storage:
            payload = await self.external_storage._store_payload(payload)
        return payload

    async def _transform_outbound_payloads(
        self, payloads: temporalio.api.common.v1.Payloads
    ):
        if self.payload_codec:
            await self.payload_codec.encode_wrapper(payloads)
        if self.external_storage:
            await self.external_storage._store_payloads(payloads)

    async def _transform_inbound_payload(
        self, payload: temporalio.api.common.v1.Payload
    ) -> temporalio.api.common.v1.Payload:
        if self.external_storage:
            payload = await self.external_storage._retrieve_payload(payload)
        if self.payload_codec:
            payload = (await self.payload_codec.decode([payload]))[0]
        return payload

    async def _transform_inbound_payloads(
        self, payloads: temporalio.api.common.v1.Payloads
    ):
        if self.external_storage:
            await self.external_storage._retrieve_payloads(payloads)
        else:
            if any(_is_reference_payload(p) for p in payloads.payloads):
                raise RuntimeError(
                    "[TMPRL1105] Detected externally stored payload(s) but external storage is not configured."
                )
        if self.payload_codec:
            await self.payload_codec.decode_wrapper(payloads)

    async def _encode_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        """Codec encode only."""
        encoded_payloads = list(payloads)
        if self.payload_codec:
            encoded_payloads = await self.payload_codec.encode(encoded_payloads)
        return encoded_payloads

    async def _external_store_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        """External storage store, then validate payload limits."""
        stored_payloads = list(payloads)
        if self.external_storage:
            stored_payloads = await self.external_storage._store_payload_sequence(
                stored_payloads
            )
        return stored_payloads

    async def _external_retrieve_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        """External storage retrieve only."""
        retrieved_payloads = list(payloads)
        if self.external_storage:
            retrieved_payloads = await self.external_storage._retrieve_payload_sequence(
                retrieved_payloads
            )
        else:
            if any(_is_reference_payload(p) for p in retrieved_payloads):
                raise RuntimeError(
                    "[TMPRL1105] Detected externally stored payload(s) but external storage is not configured."
                )
        return retrieved_payloads

    async def _decode_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        """Codec decode only."""
        decoded_payloads = list(payloads)
        if self.payload_codec:
            decoded_payloads = await self.payload_codec.decode(decoded_payloads)
        return decoded_payloads

    # Temporary shortcircuit detection while the _decode_* methods may no-op if
    # a payload codec is not configured. Remove once those paths have more to them.
    @property
    def _decode_payload_has_effect(self) -> bool:
        return self.payload_codec is not None or self.external_storage is not None


def default() -> DataConverter:
    """Default data converter.

    .. deprecated::
        Use :py:meth:`DataConverter.default` instead.
    """
    return DataConverter.default
