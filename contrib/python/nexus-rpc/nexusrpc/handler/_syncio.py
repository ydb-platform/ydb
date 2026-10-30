from __future__ import annotations

from typing import (
    Callable,
)

from nexusrpc._common import InputT, OutputT
from nexusrpc._util import (
    is_async_callable,
)
from nexusrpc.handler._common import (
    CancelOperationContext,
    StartOperationContext,
    StartOperationResultSync,
)

from ._operation_handler import OperationHandler


class SyncOperationHandler(OperationHandler[InputT, OutputT]):
    """
    An :py:class:`nexusrpc.handler.OperationHandler` that is limited to responding synchronously.

    The name 'SyncOperationHandler' means that it responds synchronously, in the
    sense that the start method delivers the final operation result as its return
    value, rather than returning an operation token representing an in-progress
    operation.

    This version of the class uses `def` methods. For the async version, see
    :py:class:`nexusrpc.handler.SyncOperationHandler`.
    """

    def __init__(self, start: Callable[[StartOperationContext, InputT], OutputT]):
        if is_async_callable(start):
            raise RuntimeError(
                f"{start} is an `async def` method. "
                "SyncOperationHandler must be initialized with a `def` method. "
                "To use `async def` methods, use nexusrpc.handler.SyncOperationHandler."
            )
        self._start = start
        if start.__doc__:
            if start_func := getattr(self.start, "__func__", None):
                start_func.__doc__ = start.__doc__

    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> StartOperationResultSync[OutputT]:
        """
        Start the operation and return its final result synchronously.
        """
        return StartOperationResultSync(self._start(ctx, input))

    def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )
