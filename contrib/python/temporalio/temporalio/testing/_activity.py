"""Activity test environment."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

from typing_extensions import ParamSpec

import temporalio.activity
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.worker._activity
from temporalio.client import Client

_Params = ParamSpec("_Params")
_Return = TypeVar("_Return")


class ActivityEnvironment:
    """Activity environment for testing activities.

    This environment is used for running activity code that can access the
    functions in the :py:mod:`temporalio.activity` module. Use :py:meth:`run` to
    run an activity function or any function within an activity context.

    Attributes:
        info: The info that is returned from :py:func:`temporalio.activity.info`
            function. To customize, use :py:meth:`default_info` with
            :py:func:`dataclasses.replace` to modify fields.
        on_heartbeat: Function called on each heartbeat invocation by the
            activity.
        payload_converter: Payload converter set on the activity context. This
            must be set before :py:meth:`run`. Changes after the activity has
            started do not take effect.
        metric_meter: Metric meter set on the activity context. This must be set
            before :py:meth:`run`. Changes after the activity has started do not
            take effect. Default is noop.
    """

    def __init__(self, client: Client | None = None) -> None:
        """Create an ActivityEnvironment for running activity code."""
        self.info = ActivityEnvironment.default_info()
        self.on_heartbeat: Callable[..., None] = lambda *args: None
        self.payload_converter = (
            temporalio.converter.DataConverter.default.payload_converter
        )
        self.metric_meter = temporalio.common.MetricMeter.noop
        self._cancelled = False
        self._worker_shutdown = False
        self._activities: set[_Activity] = set()
        self._client = client
        self._cancellation_details = (
            temporalio.activity._ActivityCancellationDetailsHolder()
        )

    @staticmethod
    def default_info() -> temporalio.activity.Info:
        """Get the default activity info used for testing.

        Returns a new default Info instance that can be modified using
        :py:func:`dataclasses.replace` before assigning to the info attribute.
        """
        utc_zero = datetime.fromtimestamp(0).replace(tzinfo=timezone.utc)
        return temporalio.activity.Info(
            activity_id="test",
            activity_type="unknown",
            attempt=1,
            current_attempt_scheduled_time=utc_zero,
            heartbeat_details=[],
            heartbeat_timeout=None,
            is_local=False,
            namespace="default",
            schedule_to_close_timeout=timedelta(seconds=1),
            scheduled_time=utc_zero,
            start_to_close_timeout=timedelta(seconds=1),
            started_time=utc_zero,
            task_queue="test",
            task_token=b"test",
            workflow_id="test",
            workflow_namespace="default",
            workflow_run_id="test-run",
            workflow_type="test",
            priority=temporalio.common.Priority.default,
            retry_policy=None,
            activity_run_id=None,
        )

    def cancel(
        self,
        cancellation_details: temporalio.activity.ActivityCancellationDetails = temporalio.activity.ActivityCancellationDetails(
            cancel_requested=True
        ),
    ) -> None:
        """Cancel the activity.

        Args:
            cancellation_details: details about the cancellation. These will
                be accessible through temporalio.activity.cancellation_details()
                in the activity after cancellation.

        This only has an effect on the first call.
        """
        if self._cancelled:
            return
        self._cancelled = True
        self._cancellation_details.details = cancellation_details
        for act in self._activities:
            act.cancel()

    def worker_shutdown(self) -> None:
        """Notify the activity that the worker is shutting down.

        This only has an effect on the first call.
        """
        if self._worker_shutdown:
            return
        self._worker_shutdown = True
        for act in self._activities:
            act.worker_shutdown()

    def run(
        self,
        fn: Callable[_Params, _Return],
        *args: _Params.args,
        **kwargs: _Params.kwargs,
    ) -> _Return:
        """Run the given callable in an activity context.

        Args:
            fn: The function/callable to run.
            args: All positional arguments to the callable.
            kwargs: All keyword arguments to the callable.

        Returns:
            The callable's result.
        """
        # Create an activity and run it
        return _Activity(self, fn, self._client).run(*args, **kwargs)


class _Activity:
    def __init__(
        self,
        env: ActivityEnvironment,
        fn: Callable,
        client: Client | None,
    ) -> None:
        self.env = env
        self.fn = fn
        self.is_async = inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
            fn.__call__  # type: ignore
        )
        self.cancel_thread_raiser: None | (
            temporalio.worker._activity._ThreadExceptionRaiser
        ) = None
        if not self.is_async:
            # If there is a definition and they disable thread raising, don't
            # set
            defn = temporalio.activity._Definition.from_callable(fn)
            if not defn or not defn.no_thread_cancel_exception:
                self.cancel_thread_raiser = (
                    temporalio.worker._activity._ThreadExceptionRaiser()
                )
        # Create context
        self.context = temporalio.activity._Context(
            info=lambda: env.info,
            heartbeat=lambda *args: env.on_heartbeat(*args),
            cancelled_event=temporalio.common._CompositeEvent(
                thread_event=threading.Event(),
                async_event=asyncio.Event() if self.is_async else None,
            ),
            worker_shutdown_event=temporalio.common._CompositeEvent(
                thread_event=threading.Event(),
                async_event=asyncio.Event() if self.is_async else None,
            ),
            shield_thread_cancel_exception=(
                None
                if not self.cancel_thread_raiser
                else self.cancel_thread_raiser.shielded
            ),
            payload_converter_class_or_instance=env.payload_converter,
            runtime_metric_meter=env.metric_meter,
            client=client if self.is_async else None,
            cancellation_details=env._cancellation_details,
        )
        self.task: asyncio.Task | None = None

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if self.cancel_thread_raiser:
            thread_id = threading.current_thread().ident
            if thread_id is not None:
                self.cancel_thread_raiser.set_thread_id(thread_id)

        @contextmanager
        def activity_context():
            # Set cancelled and shutdown if already so in environment
            if self.env._cancelled:
                self.context.cancelled_event.set()
            if self.env._worker_shutdown:
                self.context.worker_shutdown_event.set()

            # Add activity and set context
            self.env._activities.add(self)
            token = temporalio.activity._Context.set(self.context)
            try:
                yield None
            finally:
                # Reset context and remove activity
                temporalio.activity._Context.reset(token)
                self.env._activities.remove(self)

        # Async runs inside coroutine with a cancellable task
        if self.is_async:

            async def run_async():
                with activity_context():
                    self.task = asyncio.create_task(self.fn(*args, **kwargs))
                    if self.env._cancelled:
                        self.task.cancel()
                    return await self.task

            return run_async()
        # Sync just runs normally
        with activity_context():
            return self.fn(*args, **kwargs)

    def cancel(self) -> None:
        if not self.context.cancelled_event.is_set():
            self.context.cancelled_event.set()
        if self.cancel_thread_raiser:
            self.cancel_thread_raiser.raise_in_thread(
                temporalio.exceptions.CancelledError
            )
        if self.task and not self.task.done():
            self.task.cancel()

    def worker_shutdown(self) -> None:
        if not self.context.worker_shutdown_event.is_set():
            self.context.worker_shutdown_event.set()
