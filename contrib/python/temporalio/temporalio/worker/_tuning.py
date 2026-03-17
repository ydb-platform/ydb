import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Literal, Optional, Protocol, Union, runtime_checkable

from typing_extensions import TypeAlias

import temporalio.bridge.worker
from temporalio.common import WorkerDeploymentVersion

_DEFAULT_RESOURCE_ACTIVITY_MAX = 500

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FixedSizeSlotSupplier:
    """A fixed-size slot supplier that will never issue more than a fixed number of slots."""

    num_slots: int
    """The maximum number of slots that can be issued"""


@dataclass(frozen=True)
class ResourceBasedTunerConfig:
    """Options for a :py:class:`ResourceBasedTuner` or a :py:class:`ResourceBasedSlotSupplier`.

    .. warning::
        The resource based tuner is currently experimental.
    """

    target_memory_usage: float
    """A value between 0 and 1 that represents the target (system) memory usage. It's not recommended
       to set this higher than 0.8, since how much memory a workflow may use is not predictable, and
       you don't want to encounter OOM errors."""
    target_cpu_usage: float
    """A value between 0 and 1 that represents the target (system) CPU usage. This can be set to 1.0
       if desired, but it's recommended to leave some headroom for other processes."""


@dataclass(frozen=True)
class ResourceBasedSlotConfig:
    """Options for a specific slot type being used with a :py:class:`ResourceBasedSlotSupplier`.

    .. warning::
        The resource based tuner is currently experimental.
    """

    minimum_slots: Optional[int] = None
    """Amount of slots that will be issued regardless of any other checks. Defaults to 5 for workflows and 1 for
    activities."""
    maximum_slots: Optional[int] = None
    """Maximum amount of slots permitted. Defaults to 500."""
    ramp_throttle: Optional[timedelta] = None
    """Minimum time we will wait (after passing the minimum slots number) between handing out new slots in milliseconds.
    Defaults to 0 for workflows and 50ms for activities.

    This value matters because how many resources a task will use cannot be determined ahead of time, and thus the
    system should wait to see how much resources are used before issuing more slots."""


@dataclass(frozen=True)
class ResourceBasedSlotSupplier:
    """A slot supplier that will dynamically adjust the number of slots based on resource usage.

    .. warning::
        The resource based tuner is currently experimental.
    """

    slot_config: ResourceBasedSlotConfig
    tuner_config: ResourceBasedTunerConfig
    """Options for the tuner that will be used to adjust the number of slots. When used with a
    :py:class:`CompositeTuner`, all resource-based slot suppliers must use the same tuner options."""


class SlotPermit:
    """A permit to use a slot for a workflow/activity/local activity task.

    You can inherit from this class to add your own data to the permit.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    pass


# WARNING: This must match Rust worker::SlotReserveCtx
class SlotReserveContext(Protocol):
    """Context for reserving a slot from a :py:class:`CustomSlotSupplier`.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    slot_type: Literal["workflow", "activity", "local-activity"]
    """The type of slot trying to be reserved. Always one of "workflow", "activity", or "local-activity"."""
    task_queue: str
    """The name of the task queue for which this reservation request is associated."""
    worker_identity: str
    """The identity of the worker that is requesting the reservation."""
    worker_build_id: str
    """The build id of the worker that is requesting the reservation.

    .. warning::
        Deprecated, use :py:attr:`worker_deployment_version` instead.
    """
    worker_deployment_version: Optional[WorkerDeploymentVersion]
    """The deployment version of the worker that is requesting the reservation, if any."""
    is_sticky: bool
    """True iff this is a reservation for a sticky poll for a workflow task."""


# WARNING: This must match Rust worker::WorkflowSlotInfo
@runtime_checkable
class WorkflowSlotInfo(Protocol):
    """Info about a workflow task slot usage.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    workflow_type: str
    is_sticky: bool


# WARNING: This must match Rust worker::ActivitySlotInfo
@runtime_checkable
class ActivitySlotInfo(Protocol):
    """Info about an activity task slot usage.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    activity_type: str


# WARNING: This must match Rust worker::LocalActivitySlotInfo
@runtime_checkable
class LocalActivitySlotInfo(Protocol):
    """Info about a local activity task slot usage.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    activity_type: str


SlotInfo: TypeAlias = Union[WorkflowSlotInfo, ActivitySlotInfo, LocalActivitySlotInfo]


# WARNING: This must match Rust worker::SlotMarkUsedCtx
@dataclass(frozen=True)
class SlotMarkUsedContext(Protocol):
    """Context for marking a slot used from a :py:class:`CustomSlotSupplier`.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    slot_info: SlotInfo
    """Info about the task that will be using the slot."""
    permit: SlotPermit
    """The permit that was issued when the slot was reserved."""


# WARNING: This must match Rust worker::SlotReleaseCtx
@dataclass(frozen=True)
class SlotReleaseContext:
    """Context for releasing a slot from a :py:class:`CustomSlotSupplier`.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    slot_info: Optional[SlotInfo]
    """Info about the task that will be using the slot. May be None if the slot was never used."""
    permit: SlotPermit
    """The permit that was issued when the slot was reserved."""


class CustomSlotSupplier(ABC):
    """This class can be implemented to provide custom slot supplier behavior.

    .. warning::
        Custom slot suppliers are currently experimental.
    """

    @abstractmethod
    async def reserve_slot(self, ctx: SlotReserveContext) -> SlotPermit:
        """This function is called before polling for new tasks. Your implementation must block until a
        slot is available then return a permit to use that slot.

        The only acceptable exception to throw is :py:class:`asyncio.CancelledError`, as invocations of this method may
        be cancelled. Any other exceptions thrown will be logged and ignored.

        It is technically possible but rare, during worker shutdown, for this method to be called and return a value,
        but the Rust Core may not have a chance to _observe_ that value. In such cases the returned permit will not be
        released. The permit will, however, be forgotten and python will garbage collect it. So if you use the same slot
        supplier over the lifetime of more than one worker and it is critically important for you to clean up some
        resources associated all permits you construct, then consider using a finalizer on your returned permits.

        Args:
            ctx: The context for slot reservation.

        Returns:
            A permit to use the slot which may be populated with your own data.
        """
        ...

    @abstractmethod
    def try_reserve_slot(self, ctx: SlotReserveContext) -> Optional[SlotPermit]:
        """This function is called when trying to reserve slots for "eager" workflow and activity tasks.
        Eager tasks are those which are returned as a result of completing a workflow task, rather than
        from polling. Your implementation must not block, and if a slot is available, return a permit
        to use that slot.

        Args:
            ctx: The context for slot reservation.

        Returns:
            Maybe a permit to use the slot which may be populated with your own data.
        """
        ...

    @abstractmethod
    def mark_slot_used(self, ctx: SlotMarkUsedContext) -> None:
        """This function is called once a slot is actually being used to process some task, which may be
        some time after the slot was reserved originally. For example, if there is no work for a
        worker, a number of slots equal to the number of active pollers may already be reserved, but
        none of them are being used yet. This call should be non-blocking.

        Args:
            ctx: The context for marking a slot as used.
        """
        ...

    @abstractmethod
    def release_slot(self, ctx: SlotReleaseContext) -> None:
        """This function is called once a permit is no longer needed. This could be because the task has
        finished, whether successfully or not, or because the slot was no longer needed (ex: the number
        of active pollers decreased). This call should be non-blocking.

        Args:
            ctx: The context for releasing a slot.
        """
        ...


SlotSupplier: TypeAlias = Union[
    FixedSizeSlotSupplier, ResourceBasedSlotSupplier, CustomSlotSupplier
]


class _BridgeSlotSupplierWrapper:
    def __init__(self, supplier: CustomSlotSupplier):
        self._supplier = supplier

    async def reserve_slot(
        self, ctx: SlotReserveContext, reserve_cb: Callable[[Any], None]
    ) -> SlotPermit:
        try:
            reserve_fut = asyncio.create_task(self._supplier.reserve_slot(ctx))
            reserve_cb(reserve_fut)
            return await reserve_fut
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Error in custom slot supplier `reserve_slot`", exc_info=True
            )
            # Error needs to be re-thrown here so the rust code will loop
            raise

    def try_reserve_slot(self, ctx: SlotReserveContext) -> Optional[SlotPermit]:
        try:
            return self._supplier.try_reserve_slot(ctx)
        except Exception:
            logger.warning(
                "Error in custom slot supplier `try_reserve_slot`", exc_info=True
            )
            return None

    def release_slot(self, ctx: SlotReleaseContext) -> None:
        try:
            self._supplier.release_slot(ctx)
        except Exception:
            logger.warning(
                "Error in custom slot supplier `release_slot`", exc_info=True
            )

    def mark_slot_used(self, ctx: SlotMarkUsedContext) -> None:
        try:
            self._supplier.mark_slot_used(ctx)
        except Exception:
            logger.warning(
                "Error in custom slot supplier `mark_slot_used`", exc_info=True
            )


def _to_bridge_slot_supplier(
    slot_supplier: SlotSupplier, kind: Literal["workflow", "activity", "local_activity"]
) -> temporalio.bridge.worker.SlotSupplier:
    if isinstance(slot_supplier, FixedSizeSlotSupplier):
        return temporalio.bridge.worker.FixedSizeSlotSupplier(slot_supplier.num_slots)
    elif isinstance(slot_supplier, ResourceBasedSlotSupplier):
        min_slots = 5 if kind == "workflow" else 1
        max_slots = _DEFAULT_RESOURCE_ACTIVITY_MAX
        ramp_throttle = (
            timedelta(seconds=0) if kind == "workflow" else timedelta(milliseconds=50)
        )
        if slot_supplier.slot_config.minimum_slots is not None:
            min_slots = slot_supplier.slot_config.minimum_slots
        if slot_supplier.slot_config.maximum_slots is not None:
            max_slots = slot_supplier.slot_config.maximum_slots
        if slot_supplier.slot_config.ramp_throttle is not None:
            ramp_throttle = slot_supplier.slot_config.ramp_throttle
        return temporalio.bridge.worker.ResourceBasedSlotSupplier(
            min_slots,
            max_slots,
            int(ramp_throttle / timedelta(milliseconds=1)),
            temporalio.bridge.worker.ResourceBasedTunerConfig(
                slot_supplier.tuner_config.target_memory_usage,
                slot_supplier.tuner_config.target_cpu_usage,
            ),
        )
    elif isinstance(slot_supplier, CustomSlotSupplier):
        return temporalio.bridge.worker.BridgeCustomSlotSupplier(
            _BridgeSlotSupplierWrapper(slot_supplier)
        )
    else:
        raise TypeError(f"Unknown slot supplier type: {slot_supplier}")


class WorkerTuner(ABC):
    """WorkerTuners allow for the dynamic customization of some aspects of worker configuration"""

    @staticmethod
    def create_resource_based(
        *,
        target_memory_usage: float,
        target_cpu_usage: float,
        workflow_config: Optional[ResourceBasedSlotConfig] = None,
        activity_config: Optional[ResourceBasedSlotConfig] = None,
        local_activity_config: Optional[ResourceBasedSlotConfig] = None,
    ) -> "WorkerTuner":
        """Create a resource-based tuner with the provided options."""
        resource_cfg = ResourceBasedTunerConfig(target_memory_usage, target_cpu_usage)
        wf = ResourceBasedSlotSupplier(
            workflow_config or ResourceBasedSlotConfig(), resource_cfg
        )
        act = ResourceBasedSlotSupplier(
            activity_config or ResourceBasedSlotConfig(), resource_cfg
        )
        local_act = ResourceBasedSlotSupplier(
            local_activity_config or ResourceBasedSlotConfig(), resource_cfg
        )
        return _CompositeTuner(
            wf,
            act,
            local_act,
        )

    @staticmethod
    def create_fixed(
        *,
        workflow_slots: Optional[int],
        activity_slots: Optional[int],
        local_activity_slots: Optional[int],
    ) -> "WorkerTuner":
        """Create a fixed-size tuner with the provided number of slots. Any unspecified slots will default to 100."""
        return _CompositeTuner(
            FixedSizeSlotSupplier(workflow_slots if workflow_slots else 100),
            FixedSizeSlotSupplier(activity_slots if activity_slots else 100),
            FixedSizeSlotSupplier(
                local_activity_slots if local_activity_slots else 100
            ),
        )

    @staticmethod
    def create_composite(
        *,
        workflow_supplier: SlotSupplier,
        activity_supplier: SlotSupplier,
        local_activity_supplier: SlotSupplier,
    ) -> "WorkerTuner":
        """Create a tuner composed of the provided slot suppliers."""
        return _CompositeTuner(
            workflow_supplier,
            activity_supplier,
            local_activity_supplier,
        )

    @abstractmethod
    def _get_workflow_task_slot_supplier(self) -> SlotSupplier:
        raise NotImplementedError

    @abstractmethod
    def _get_activity_task_slot_supplier(self) -> SlotSupplier:
        raise NotImplementedError

    @abstractmethod
    def _get_local_activity_task_slot_supplier(self) -> SlotSupplier:
        raise NotImplementedError

    def _to_bridge_tuner(self) -> temporalio.bridge.worker.TunerHolder:
        return temporalio.bridge.worker.TunerHolder(
            _to_bridge_slot_supplier(
                self._get_workflow_task_slot_supplier(), "workflow"
            ),
            _to_bridge_slot_supplier(
                self._get_activity_task_slot_supplier(), "activity"
            ),
            _to_bridge_slot_supplier(
                self._get_local_activity_task_slot_supplier(), "local_activity"
            ),
        )

    def _get_activities_max(self) -> Optional[int]:
        ss = self._get_activity_task_slot_supplier()
        if isinstance(ss, FixedSizeSlotSupplier):
            return ss.num_slots
        elif isinstance(ss, ResourceBasedSlotSupplier):
            return ss.slot_config.maximum_slots or _DEFAULT_RESOURCE_ACTIVITY_MAX
        return None


@dataclass(frozen=True)
class _CompositeTuner(WorkerTuner):
    """This tuner allows for different slot suppliers for different slot types."""

    workflow_slot_supplier: SlotSupplier
    activity_slot_supplier: SlotSupplier
    local_activity_slot_supplier: SlotSupplier

    def _get_workflow_task_slot_supplier(self) -> SlotSupplier:
        return self.workflow_slot_supplier

    def _get_activity_task_slot_supplier(self) -> SlotSupplier:
        return self.activity_slot_supplier

    def _get_local_activity_task_slot_supplier(self) -> SlotSupplier:
        return self.local_activity_slot_supplier
