"""System Nexus operation helpers.

.. warning::
    This API is experimental and subject to change.
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

import temporalio.api.common.v1
import temporalio.common
import temporalio.converter
from temporalio.bridge._visitor_functions import VisitorFunctions
from temporalio.converter import BinaryProtoPayloadConverter, CompositePayloadConverter
from temporalio.converter._payload_converter import (
    _TemporalTransferTypePayloadConverter,
)

TEMPORAL_SYSTEM_ENDPOINT = "__temporal_system"


@dataclass(frozen=True)
class _SystemNexusUserConverters:
    payload_converter: temporalio.converter.PayloadConverter
    failure_converter: temporalio.converter.FailureConverter


_user_converters: contextvars.ContextVar[_SystemNexusUserConverters | None] = (
    contextvars.ContextVar("temporal-system-nexus-user-converters", default=None)
)
_SYSTEM_PAYLOAD_METADATA_KEY = "__temporal_system_payload"
_SYSTEM_PAYLOAD_METADATA_VALUE = b"true"


@contextlib.contextmanager
def _user_converter_context(
    converters: _SystemNexusUserConverters,
) -> Iterator[None]:
    """Set the user converters for system Nexus model conversion."""
    token = _user_converters.set(converters)
    try:
        yield
    finally:
        _user_converters.reset(token)


def _current_user_converters() -> _SystemNexusUserConverters:
    converters = _user_converters.get()
    if converters is None:
        raise RuntimeError("System Nexus user converter context is not active")
    return converters


def _current_user_payload_converter() -> temporalio.converter.PayloadConverter:  # pyright: ignore[reportUnusedFunction]
    """Return the active user payload converter for system Nexus model conversion."""
    return _current_user_converters().payload_converter


def _current_user_failure_converter() -> temporalio.converter.FailureConverter:  # pyright: ignore[reportUnusedFunction]
    """Return the active user failure converter for system Nexus model conversion."""
    return _current_user_converters().failure_converter


class _SystemNexusOuterPayloadConverter(CompositePayloadConverter):
    """Payload converter for system Nexus outer proto envelopes."""

    def __init__(self) -> None:
        """Create a payload converter for system Nexus outer envelopes."""
        super().__init__(BinaryProtoPayloadConverter())

    def to_payloads(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
        """See base class."""
        payloads = super().to_payloads(values)
        for value, payload in zip(values, payloads):
            if isinstance(value, temporalio.common.RawValue):
                continue
            payload.metadata[_SYSTEM_PAYLOAD_METADATA_KEY] = (
                _SYSTEM_PAYLOAD_METADATA_VALUE
            )
        return payloads


class _SystemNexusPayloadConverter(temporalio.converter.PayloadConverter):
    """Payload converter for system Nexus outer envelopes."""

    _user_converters: _SystemNexusUserConverters
    _outer_payload_converter: temporalio.converter.PayloadConverter

    def __init__(
        self,
        user_payload_converter: temporalio.converter.PayloadConverter,
        user_failure_converter: temporalio.converter.FailureConverter,
    ) -> None:
        """Create a payload converter for system Nexus outer envelopes."""
        self._user_converters = _SystemNexusUserConverters(
            user_payload_converter, user_failure_converter
        )
        self._outer_payload_converter = _TemporalTransferTypePayloadConverter.wrap(
            _SystemNexusOuterPayloadConverter()
        )

    def to_payloads(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
        """See base class."""
        with _user_converter_context(self._user_converters):
            return self._outer_payload_converter.to_payloads(values)

    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[Any]:
        """See base class."""
        with _user_converter_context(self._user_converters):
            return self._outer_payload_converter.from_payloads(payloads, type_hints)


def is_system_endpoint(endpoint: str) -> bool:
    """Return whether a Nexus endpoint is the Temporal system endpoint.

    .. warning::
        This API is experimental and subject to change.
    """
    return endpoint == TEMPORAL_SYSTEM_ENDPOINT


def _is_system_payload(payload: temporalio.api.common.v1.Payload) -> bool:
    return (
        payload.metadata.get(_SYSTEM_PAYLOAD_METADATA_KEY)
        == _SYSTEM_PAYLOAD_METADATA_VALUE
    )


async def maybe_visit_payload(
    payload: temporalio.api.common.v1.Payload,
    visitor_functions: VisitorFunctions,
    skip_search_attributes: bool,
) -> temporalio.api.common.v1.Payload | None:
    """Visit nested payloads if the payload is a Temporal system Nexus envelope."""
    if not _is_system_payload(payload):
        return None

    payload_converter = _SystemNexusOuterPayloadConverter()
    value = payload_converter.from_payload(payload)
    from temporalio.bridge._visitor import PayloadVisitor

    payload_visitor = PayloadVisitor(skip_search_attributes=skip_search_attributes)
    checkpoint = visitor_functions.checkpoint()
    await payload_visitor.visit(visitor_functions, value)
    if checkpoint is not None:
        await visitor_functions.drain_since(checkpoint)
    return payload_converter.to_payload(value)


def _get_payload_converter(  # pyright: ignore[reportUnusedFunction]
    user_payload_converter: temporalio.converter.PayloadConverter,
    user_failure_converter: temporalio.converter.FailureConverter,
) -> temporalio.converter.PayloadConverter:
    """Return the fixed payload converter for system Nexus outer envelopes."""
    return _SystemNexusPayloadConverter(user_payload_converter, user_failure_converter)


def _get_serialization_context(  # pyright: ignore[reportUnusedFunction]
    service: str,
    operation: str,
    request: Any,
) -> temporalio.converter.SerializationContext | None:
    """Return the serialization context for a system Nexus operation."""
    from .workflow_service import __nexus_operation_registry__

    operation_info = __nexus_operation_registry__.get((service, operation))
    if operation_info is None or operation_info.serialization_context is None:
        return None
    return operation_info.serialization_context(request)


__all__ = [
    "TEMPORAL_SYSTEM_ENDPOINT",
    "is_system_endpoint",
]
