from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING

from temporalio.client import WorkflowHistory

if TYPE_CHECKING:
    from temporalio.worker import (
        Replayer,
        ReplayerConfig,
        Worker,
        WorkerConfig,
        WorkflowReplayResult,
    )


class Plugin(abc.ABC):
    """Base class for worker plugins that can intercept and modify worker behavior.

    Plugins allow customization of worker creation and execution processes
    through a chain of responsibility pattern. Each plugin can modify the worker
    configuration or intercept worker execution.

    WARNING: This is an experimental feature and may change in the future.
    """

    def name(self) -> str:
        """Get the qualified name of this plugin. Can be overridden if desired to provide a more appropriate name.

        Returns:
            The fully qualified name of the plugin class (module.classname).
        """
        return type(self).__module__ + "." + type(self).__qualname__

    @abc.abstractmethod
    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Hook called when creating a worker to allow modification of configuration.

        This method is called during worker creation and allows plugins to modify
        the worker configuration before the worker is fully initialized. Plugins
        can modify task queue names, adjust concurrency settings, add interceptors,
        or change other worker settings.

        Args:
            config: The worker configuration dictionary to potentially modify.

        Returns:
            The modified worker configuration.
        """

    @abc.abstractmethod
    async def run_worker(
        self, worker: Worker, next: Callable[[Worker], Awaitable[None]]
    ) -> None:
        """Hook called when running a worker to allow interception of execution.

        This method is called when the worker is started and allows plugins to
        intercept or wrap the worker execution. Plugins can add monitoring,
        custom lifecycle management, or other execution-time behavior.

        Args:
            worker: The worker instance to run.
            next: Callable to continue the worker execution.
        """

    @abc.abstractmethod
    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Hook called when creating a replayer to allow modification of configuration.

        This should be used to configure anything in ReplayerConfig needed to make execution match
        the worker and client config. This could include interceptors, DataConverter, workflows, and more.

        Args:
            config: The replayer configuration dictionary to potentially modify.

        Returns:
            The modified replayer configuration.
        """

    @abc.abstractmethod
    def run_replayer(
        self,
        replayer: Replayer,
        histories: AsyncIterator[WorkflowHistory],
        next: Callable[
            [Replayer, AsyncIterator[WorkflowHistory]],
            AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]],
        ],
    ) -> AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]]:
        """Hook called when running a replayer to allow interception of execution."""
