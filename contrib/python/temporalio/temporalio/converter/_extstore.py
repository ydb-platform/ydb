"""External payload storage support for offloading payloads to external storage
systems.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine, Generator, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar, TypeVar

from typing_extensions import Self

from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.sdk.v1.external_storage_pb2 import ExternalStorageReference
from temporalio.converter._payload_converter import (
    JSONPlainPayloadConverter,
    JSONProtoPayloadConverter,
)

_T = TypeVar("_T")

_REFERENCE_ENCODING = b"json/external-storage-reference"
_REFERENCE_MESSAGE_TYPE = ExternalStorageReference.DESCRIPTOR.full_name.encode()


@dataclass
class StorageOperationMetrics:
    """Accumulates metrics from external storage operations."""

    payload_count: int = 0
    """Number of payloads stored or retrieved externally."""

    total_size: int = 0
    """Total size in bytes of externally stored/retrieved payloads."""

    total_duration: timedelta = dataclasses.field(default_factory=timedelta)
    """Wall-clock time spent on external storage operations."""

    driver_names: set[str] = dataclasses.field(default_factory=set)
    """Names of the drivers that participated in the operations."""

    def record_batch(
        self, count: int, size: int, duration: timedelta, driver_names: set[str]
    ) -> None:
        """Record metrics from a batch of storage operations."""
        self.payload_count += count
        self.total_size += size
        self.total_duration += duration
        self.driver_names.update(driver_names)

    @contextlib.contextmanager
    def track(self) -> Generator[Self, None, None]:
        """Set this instance as the current metrics context and reset on exit."""
        token = _current_storage_metrics.set(self)
        try:
            yield self
        finally:
            _current_storage_metrics.reset(token)


_current_storage_metrics: contextvars.ContextVar[StorageOperationMetrics | None] = (
    contextvars.ContextVar("_current_storage_metrics", default=None)
)


async def _gather_cancel_on_error(
    coros: Sequence[Coroutine[Any, Any, _T]],
) -> list[_T]:
    """Run coroutines concurrently; cancel all remaining tasks if any one fails."""
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@dataclass(frozen=True)
class StorageDriverClaim:
    """A driver-defined reference to an externally-stored payload that can be used to
    retrieve it.

    .. warning::
        This API is experimental.
    """

    claim_data: Mapping[str, str]
    """Driver-defined data for identifying and retrieving an externally stored
    payload.
    """


@dataclass(frozen=True, kw_only=True)
class StorageDriverWorkflowInfo:
    """Workflow identity information for external storage operations.

    .. warning::
        This API is experimental.
    """

    namespace: str
    """The namespace of the workflow execution."""

    id: str | None = None
    """The workflow ID."""

    run_id: str | None = None
    """The workflow run ID, if available."""

    type: str | None = None
    """The workflow type name, if available."""


@dataclass(frozen=True, kw_only=True)
class StorageDriverActivityInfo:
    """Activity identity information for external storage operations.

    .. warning::
        This API is experimental.
    """

    namespace: str
    """The namespace of the activity execution."""

    id: str | None = None
    """The activity ID."""

    run_id: str | None = None
    """The activity run ID (only for standalone activities)."""

    type: str | None = None
    """The activity type name, if available."""


@dataclass(frozen=True)
class StorageDriverStoreContext:
    """Context passed to :meth:`StorageDriver.store` and ``driver_selector`` calls.

    .. warning::
        This API is experimental.
    """

    target: StorageDriverActivityInfo | StorageDriverWorkflowInfo | None = None
    """The workflow or activity for which this payload is being stored.

    For payloads being stored on behalf of an explicit target (e.g. a child
    workflow being started, an activity being scheduled, an external workflow
    being signaled), this is that target's identity.  When no explicit target
    exists the current execution context (workflow or activity) is used as the
    target instead."""


@dataclass(frozen=True)
class StorageDriverRetrieveContext:
    """Context passed to :meth:`StorageDriver.retrieve` calls.

    .. warning::
        This API is experimental.
    """


class StorageDriver(ABC):
    """Base driver for storing and retrieve payloads from external storage systems.

    .. warning::
        This API is experimental.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of this driver instance. A driver may allow
        its name to be parameterized at construction time so that multiple
        instances of the same driver class can coexist in
        :attr:`ExternalStorage.drivers` with distinct names.
        """
        raise NotImplementedError

    def type(self) -> str:
        """Returns the type of the storage driver. This string should be
        the same across all instantiations of the same driver class. This
        allows the equivalent driver implementation in different languages
        to be named the same.

        Defaults to the class name. Subclasses may override this to return a
        stable, language-agnostic identifier.
        """
        return type(self).__name__

    @abstractmethod
    async def store(
        self,
        context: StorageDriverStoreContext,
        payloads: Sequence[Payload],
    ) -> list[StorageDriverClaim]:
        """Stores payloads in external storage and returns a
        :class:`StorageDriverClaim` for each one. The returned list must be the
        same length as ``payloads``.
        """
        raise NotImplementedError

    @abstractmethod
    async def retrieve(
        self,
        context: StorageDriverRetrieveContext,
        claims: Sequence[StorageDriverClaim],
    ) -> list[Payload]:
        """Retrieves payloads from external storage for the given
        :class:`StorageDriverClaim` list. The returned list must be the same
        length as ``claims``.
        """
        raise NotImplementedError


class StorageWarning(RuntimeWarning):
    """Warning for external storage issues.

    .. warning::
        This API is experimental.
    """


@dataclass(frozen=True)
class _StorageReference:
    """Legacy external storage reference used only on the retrieval path as a
    fallback for in-flight workflows that were written before the
    ExternalStorageReference proto was introduced.
    """

    driver_name: str
    driver_claim: StorageDriverClaim


@dataclass(frozen=True)
class ExternalStorage:
    """Configuration for external storage behavior.

    .. warning::
        This API is experimental.
    """

    drivers: Sequence[StorageDriver]
    """Drivers available for storing and retrieving payloads. At least one
    driver must be provided. If more than one driver is registered,
    :attr:`driver_selector` must also be set.

    Drivers in this list are looked up by :meth:`StorageDriver.name` during
    retrieval, so each driver must have a unique name.
    """

    driver_selector: (
        Callable[[StorageDriverStoreContext, Payload], StorageDriver | None] | None
    ) = None
    """Controls which driver stores a given payload. A callable that returns the
    driver instance to use, or ``None`` to leave the payload stored inline.
    The returned driver must be one of the instances registered in
    :attr:`drivers`.

    Required when more than one driver is registered. When ``None`` and only
    one driver is registered, that driver is used for all store operations.
    """

    payload_size_threshold: int = 256 * 1024
    """Minimum payload size in bytes before external storage is considered.
    Defaults to 256 KiB. Must be greater than or equal to zero.
    """

    _driver_map: dict[str, StorageDriver] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    """Name-keyed index of :attr:`drivers`, built at construction time. Used
    for retrieval lookups.
    """

    _store_context: StorageDriverStoreContext = dataclasses.field(
        default=StorageDriverStoreContext(target=None),
        init=False,
        repr=False,
        compare=False,
    )
    """Store context bound to this instance via :meth:`_with_store_context`."""

    _claim_converter: ClassVar[JSONProtoPayloadConverter] = JSONProtoPayloadConverter()
    _legacy_claim_converter: ClassVar[JSONPlainPayloadConverter] = (
        JSONPlainPayloadConverter(encoding=_REFERENCE_ENCODING.decode())
    )

    def __post_init__(self) -> None:
        """Validate drivers and build the internal name-keyed driver map.

        Raises :exc:`ValueError` if no drivers are provided, if
        :attr:`payload_size_threshold` is less than zero, if more than one
        driver is registered without a :attr:`driver_selector`, or if any two
        drivers share the same name.
        """
        if not self.drivers:
            raise ValueError(
                "ExternalStorage.drivers must contain at least one driver."
            )
        if self.payload_size_threshold < 0:
            raise ValueError(
                "ExternalStorage.payload_size_threshold must be greater than or equal to zero."
            )
        if len(self.drivers) > 1 and self.driver_selector is None:
            raise ValueError(
                "ExternalStorage.driver_selector must be specified if multiple drivers are registered."
            )
        driver_map: dict[str, StorageDriver] = {}
        for driver in self.drivers:
            name = driver.name()
            if name in driver_map:
                raise ValueError(
                    f"ExternalStorage.drivers contains multiple drivers with name '{name}'. "
                    "Each driver must have a unique name."
                )
            driver_map[name] = driver
        object.__setattr__(self, "_driver_map", driver_map)

    def _select_driver(
        self, context: StorageDriverStoreContext, payload: Payload
    ) -> StorageDriver | None:
        """Returns the driver to use for this payload, or None to pass through."""
        if payload.ByteSize() < self.payload_size_threshold:
            return None
        selector = self.driver_selector
        if selector is None:
            return self.drivers[0] if self.drivers else None
        driver = selector(context, payload)
        if driver is None:
            return None
        registered = self._driver_map.get(driver.name())
        if registered is not driver:
            raise ValueError(
                f"Driver '{driver.name()}' returned by driver_selector is not registered in ExternalStorage.drivers"
            )
        return driver

    def _get_driver_by_name(self, name: str) -> StorageDriver:
        """Looks up a driver by name, raising :class:`ValueError` if not found."""
        driver = self._driver_map.get(name)
        if driver is None:
            raise ValueError(f"No driver found with name '{name}'")
        return driver

    def _with_store_context(self, ctx: StorageDriverStoreContext) -> ExternalStorage:
        """Return a copy of this instance with ``ctx`` bound as the store context."""
        result = dataclasses.replace(self)
        object.__setattr__(result, "_store_context", ctx)
        return result

    async def _store_payload(self, payload: Payload) -> Payload:
        start_time = time.monotonic()

        driver = self._select_driver(self._store_context, payload)
        if driver is None:
            return payload

        claims = await driver.store(self._store_context, [payload])

        self._validate_claim_length(claims, expected=1, driver=driver)

        external_size = payload.ByteSize()
        reference = ExternalStorageReference(
            driver_name=driver.name(),
            claim_data=claims[0].claim_data,
        )
        reference_payload = self._claim_converter.to_payload(reference)
        if reference_payload is None:
            raise ValueError(
                f"Failed to serialize storage reference for driver '{driver.name()}'"
            )
        reference_payload.external_payloads.add().size_bytes = external_size

        ExternalStorage._record_metrics(1, external_size, start_time, {driver.name()})

        return reference_payload

    async def _store_payloads(self, payloads: Payloads):
        stored_payloads = await self._store_payload_sequence(payloads.payloads)
        for i, payload in enumerate(stored_payloads):
            payloads.payloads[i].CopyFrom(payload)

    async def _store_payload_sequence(
        self,
        payloads: Sequence[Payload],
    ) -> list[Payload]:
        if len(payloads) == 1:
            return [await self._store_payload(payloads[0])]

        start_time = time.monotonic()

        results = list(payloads)

        to_store: list[tuple[int, Payload, StorageDriver]] = []
        for index, payload in enumerate(payloads):
            driver = self._select_driver(self._store_context, payload)
            if driver is None:
                continue
            to_store.append((index, payload, driver))

        if not to_store:
            return results

        driver_groups: dict[StorageDriver, list[tuple[int, Payload]]] = {}
        for orig_index, payload, driver in to_store:
            driver_groups.setdefault(driver, []).append((orig_index, payload))

        driver_group_list = list(driver_groups.items())

        all_claims = await _gather_cancel_on_error(
            [
                driver.store(self._store_context, [p for _, p in indexed_payloads])
                for driver, indexed_payloads in driver_group_list
            ]
        )

        external_count = 0
        external_size = 0
        driver_names: set[str] = set()
        for (driver, indexed_payloads), claims in zip(driver_group_list, all_claims):
            indices = [idx for idx, _ in indexed_payloads]
            sizes = [p.ByteSize() for _, p in indexed_payloads]

            self._validate_claim_length(claims, expected=len(indices), driver=driver)

            for i, claim in enumerate(claims):
                reference = ExternalStorageReference(
                    driver_name=driver.name(),
                    claim_data=claim.claim_data,
                )
                reference_payload = self._claim_converter.to_payload(reference)
                if reference_payload is None:
                    raise ValueError(
                        f"Failed to serialize storage reference for driver '{driver.name()}'"
                    )
                reference_payload.external_payloads.add().size_bytes = sizes[i]
                results[indices[i]] = reference_payload
                external_size += sizes[i]

            external_count += len(claims)
            driver_names.add(driver.name())

        ExternalStorage._record_metrics(
            external_count, external_size, start_time, driver_names
        )

        return results

    def _decode_reference(self, payload: Payload) -> ExternalStorageReference | None:
        """Decode an external storage reference from a payload."""
        encoding = payload.metadata.get("encoding", b"")
        if encoding == _REFERENCE_ENCODING:
            legacy = self._legacy_claim_converter.from_payload(
                payload, _StorageReference
            )
            if not isinstance(legacy, _StorageReference):
                return None
            return ExternalStorageReference(
                driver_name=legacy.driver_name,
                claim_data=legacy.driver_claim.claim_data,
            )
        if not (
            encoding == b"json/protobuf"
            and payload.metadata.get("messageType") == _REFERENCE_MESSAGE_TYPE
        ):
            return None
        ref = self._claim_converter.from_payload(payload, ExternalStorageReference)
        return ref if isinstance(ref, ExternalStorageReference) else None

    async def _retrieve_payload(self, payload: Payload) -> Payload:
        ref = self._decode_reference(payload)
        if ref is None:
            return payload

        start_time = time.monotonic()
        driver = self._get_driver_by_name(ref.driver_name)
        context = StorageDriverRetrieveContext()
        claim = StorageDriverClaim(claim_data=dict(ref.claim_data))

        stored_payloads = await driver.retrieve(context, [claim])

        self._validate_payload_length(stored_payloads, expected=1, driver=driver)

        stored_payload = stored_payloads[0]

        ExternalStorage._record_metrics(
            1, stored_payload.ByteSize(), start_time, {driver.name()}
        )

        return stored_payload

    async def _retrieve_payloads(self, payloads: Payloads):
        stored_payloads = await self._retrieve_payload_sequence(payloads.payloads)
        for i, payload in enumerate(stored_payloads):
            payloads.payloads[i].CopyFrom(payload)

    async def _retrieve_payload_sequence(
        self,
        payloads: Sequence[Payload],
    ) -> list[Payload]:
        if len(payloads) == 1:
            return [await self._retrieve_payload(payloads[0])]

        start_time = time.monotonic()

        results = list(payloads)

        driver_claims: dict[StorageDriver, list[tuple[int, StorageDriverClaim]]] = {}
        for index, payload in enumerate(payloads):
            ref = self._decode_reference(payload)
            if ref is None:
                continue
            driver = self._get_driver_by_name(ref.driver_name)
            claim = StorageDriverClaim(claim_data=dict(ref.claim_data))
            driver_claims.setdefault(driver, []).append((index, claim))

        if not driver_claims:
            return results

        context = StorageDriverRetrieveContext()
        stored_by_index: dict[int, Payload] = {}

        driver_claim_list = list(driver_claims.items())

        all_stored = await _gather_cancel_on_error(
            [
                driver.retrieve(context, [claim for _, claim in indexed_claims])
                for driver, indexed_claims in driver_claim_list
            ]
        )

        external_count = 0
        external_size = 0
        driver_names: set[str] = set()
        for (driver, indexed_claims), stored_payloads in zip(
            driver_claim_list, all_stored
        ):
            indices = [idx for idx, _ in indexed_claims]

            self._validate_payload_length(
                stored_payloads,
                expected=len(indexed_claims),
                driver=driver,
            )

            for idx, stored_payload in zip(indices, stored_payloads):
                stored_by_index[idx] = stored_payload
                external_size += stored_payload.ByteSize()

            external_count += len(stored_payloads)
            driver_names.add(driver.name())

        retrieve_indices = sorted(stored_by_index.keys())
        stored_list = [stored_by_index[idx] for idx in retrieve_indices]

        for i, retrieved_payload in enumerate(stored_list):
            results[retrieve_indices[i]] = retrieved_payload

        ExternalStorage._record_metrics(
            external_count, external_size, start_time, driver_names
        )

        return results

    def _validate_claim_length(
        self, claims: Sequence[StorageDriverClaim], expected: int, driver: StorageDriver
    ) -> None:
        if len(claims) != expected:
            raise ValueError(
                f"Driver '{driver.name()}' returned {len(claims)} claims, expected {expected}",
            )

    def _validate_payload_length(
        self, payloads: Sequence[Payload], expected: int, driver: StorageDriver
    ) -> None:
        if len(payloads) != expected:
            raise ValueError(
                f"Driver '{driver.name()}' returned {len(payloads)} payloads, expected {expected}",
            )

    @staticmethod
    def _record_metrics(
        count: int, size: int, start_time: float, driver_names: set[str]
    ):
        metrics = _current_storage_metrics.get()
        if metrics is not None:
            metrics.record_batch(
                count,
                size,
                timedelta(seconds=time.monotonic() - start_time),
                driver_names,
            )
