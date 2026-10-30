from __future__ import annotations

from enum import Enum, IntEnum

import temporalio.api.enums.v1
import temporalio.bridge.proto.common
import temporalio.exceptions

__all__ = [
    "NondeterminismError",
    "ReadOnlyContextError",
    "VersioningIntent",
    "ContinueAsNewVersioningBehavior",
]


class NondeterminismError(temporalio.exceptions.TemporalError):
    """Error that can be thrown during replay for non-deterministic workflow."""

    def __init__(self, message: str) -> None:
        """Initialize a nondeterminism error."""
        super().__init__(message)
        self.message = message


class ReadOnlyContextError(temporalio.exceptions.TemporalError):
    """Error thrown when trying to do mutable workflow calls in a read-only
    context like a query or update validator.
    """

    def __init__(self, message: str) -> None:
        """Initialize a read-only context error."""
        super().__init__(message)
        self.message = message


class _NotInWorkflowEventLoopError(  # pyright: ignore[reportUnusedClass]
    temporalio.exceptions.TemporalError
):
    def __init__(self, *args: object) -> None:
        super().__init__("Not in workflow event loop")
        self.message = "Not in workflow event loop"


class VersioningIntent(Enum):
    """Indicates whether the user intends certain commands to be run on a compatible worker Build
    Id version or not.

    `COMPATIBLE` indicates that the command should run on a worker with compatible version if
    possible. It may not be possible if the target task queue does not also have knowledge of the
    current worker's Build Id.

    `DEFAULT` indicates that the command should run on the target task queue's current
    overall-default Build Id.

    Where this type is accepted optionally, an unset value indicates that the SDK should choose the
    most sensible default behavior for the type of command, accounting for whether the command will
    be run on the same task queue as the current worker.

    .. deprecated::
        Use Worker Deployment versioning instead.
    """

    COMPATIBLE = 1
    DEFAULT = 2

    def _to_proto(self) -> temporalio.bridge.proto.common.VersioningIntent.ValueType:
        if self == VersioningIntent.COMPATIBLE:
            return temporalio.bridge.proto.common.VersioningIntent.COMPATIBLE
        elif self == VersioningIntent.DEFAULT:
            return temporalio.bridge.proto.common.VersioningIntent.DEFAULT
        return temporalio.bridge.proto.common.VersioningIntent.UNSPECIFIED


class ContinueAsNewVersioningBehavior(IntEnum):
    """Experimental. Optionally decide the versioning behavior that the first task of the new run should use.
    For example, choose to AutoUpgrade on continue-as-new instead of inheriting the pinned version
    of the previous run.
    """

    UNSPECIFIED = int(
        temporalio.api.enums.v1.ContinueAsNewVersioningBehavior.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_UNSPECIFIED
    )
    """An initial versioning behavior is not set, follow the existing continue-as-new inheritance semantics.
    See https://docs.temporal.io/worker-versioning#inheritance-semantics for more detail.
    """

    AUTO_UPGRADE = int(
        temporalio.api.enums.v1.ContinueAsNewVersioningBehavior.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE
    )
    """Start the new run with AutoUpgrade behavior. Use the Target Version of the workflow's task queue at
    start-time, as AutoUpgrade workflows do. After the first workflow task completes, use whatever
    Versioning Behavior the workflow is annotated with in the workflow code.
    
    Note that if the previous workflow had a Pinned override, that override will be inherited by the
    new workflow run regardless of the ContinueAsNewVersioningBehavior specified in the continue-as-new
    command. If a Pinned override is inherited by the new run, and the new run starts with AutoUpgrade
    behavior, the base version of the new run will be the Target Version as described above, but the
    effective version will be whatever is specified by the Versioning Override until the override is removed.
    """

    USE_RAMPING_VERSION = int(
        temporalio.api.enums.v1.ContinueAsNewVersioningBehavior.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_USE_RAMPING_VERSION
    )
    """Use the Ramping Version of the workflow's task queue at start time, regardless of the workflow's
    Target Version. After the first workflow task completes, the workflow will use whatever Versioning
    Behavior it is annotated with. If there is no Ramping Version by the time that the first workflow task
    is dispatched, it will be sent to the Current Version.

    It is highly discouraged to use this if the workflow is annotated with AutoUpgrade behavior, because
    this setting ONLY applies to the first task of the workflow. If, after the first task, the workflow
    is AutoUpgrade, it will behave like a normal AutoUpgrade workflow and go to the Target Version, which
    may be the Current Version instead of the Ramping Version.

    Note that if the workflow being continued has a Pinned override, that override will be inherited by the
    new workflow run regardless of the ContinueAsNewVersioningBehavior specified in the continue-as-new
    command. Versioning Override always takes precedence until it's removed manually via
    UpdateWorkflowExecutionOptions.
    """
