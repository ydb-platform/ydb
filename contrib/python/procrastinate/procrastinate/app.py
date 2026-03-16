from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from collections.abc import Iterable, Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    TypedDict,
)

from typing_extensions import NotRequired, Unpack

from procrastinate import blueprints, exceptions, jobs, manager, schema, utils
from procrastinate import connector as connector_module

if TYPE_CHECKING:
    from procrastinate import worker

logger = logging.getLogger(__name__)


class WorkerOptions(TypedDict):
    queues: NotRequired[Iterable[str]]
    name: NotRequired[str]
    concurrency: NotRequired[int]
    wait: NotRequired[bool]
    fetch_job_polling_interval: NotRequired[float]
    abort_job_polling_interval: NotRequired[float]
    shutdown_graceful_timeout: NotRequired[float]
    listen_notify: NotRequired[bool]
    delete_jobs: NotRequired[str | jobs.DeleteJobCondition]
    additional_context: NotRequired[dict[str, Any]]
    install_signal_handlers: NotRequired[bool]
    update_heartbeat_interval: NotRequired[float]
    stalled_worker_timeout: NotRequired[float]


class App(blueprints.Blueprint):
    """
    The App is the main entry point for procrastinate integration.

    Instantiate a single `App` in your code
    and use it to decorate your tasks with `App.task`.

    You can run a worker with `App.run_worker`.
    """

    @classmethod
    def from_path(cls, dotted_path: str) -> App:
        """
        Create an :py:class:`App` object by dynamically loading the
        object at the given path.

        Parameters
        ----------
        dotted_path :
            Dotted path to the object to load (e.g.
            ``mymodule.submodule.procrastinate_app``)
        """
        return utils.load_from_path(dotted_path, cls)

    def __init__(
        self,
        *,
        connector: connector_module.BaseConnector,
        import_paths: Iterable[str] | None = None,
        worker_defaults: WorkerOptions | None = None,
        periodic_defaults: dict | None = None,
    ):
        """
        Parameters
        ----------
        connector :
            Typically an `AiopgConnector`. It will be responsible for all communications
            with the database. Mandatory.
        import_paths :
            List of python dotted paths of modules to import, to make sure that
            the workers know about all possible tasks. If there are tasks in a
            module that is neither imported as a side effect of importing the
            App, nor specified in this list, and a worker encounters a task
            defined in that module, the task will fail (`TaskNotFound`). While
            it is not mandatory to specify paths to modules that you know have
            already been imported, it's a good idea to do so.
        worker_defaults :
            All the values passed here will override the default values sent when
            launching a worker. See `App.run_worker` for details.
        periodic_defaults :
            Parameters for fine tuning the periodic tasks deferrer. Available
            parameters are:

            - ``max_delay``: ``float``, in seconds. When a worker starts and there's
              a periodic task that hasn't been deferred yet, it will try to defer the task
              only if it has been overdue for less time than specified by this
              parameter. If the task has been overdue for longer, the worker will wait
              until the next scheduled execution. This mechanism prevents newly added
              periodic tasks from being immediately deferred. Additionally, it ensures
              that periodic tasks, which were not deferred due to system outages, are
              not deferred upon application recovery (provided that the outage duration
              is longer than ``max_delay``), that's especially important for tasks intended
              to run during off-peak hours, such as intensive nightly tasks. (defaults to 10 minutes)
        """

        super().__init__()

        self.connector = connector
        self.import_paths = import_paths or []
        self.worker_defaults = worker_defaults or {}
        self.periodic_defaults = periodic_defaults or {}

        #: The :py:class:`~manager.JobManager` linked to the application
        self.job_manager: manager.JobManager = manager.JobManager(
            connector=self.connector
        )

        self._register_builtin_tasks()

    def with_connector(
        self,
        connector: connector_module.BaseConnector,
    ) -> App:
        """
        Create another app instance sychronized with this one, with a different
        connector.

        .. deprecated:: 2.14.0
            Use `replace_connector` instead. Because this method creates a new
            app that references the same tasks, and the task have a link
            back to the app, using this method can lead to unexpected behavior.

        Parameters
        ----------
        connector :
            The new connector to use.

        Returns
        -------
        :
            A new app with the same tasks.
        """
        app = App(
            connector=connector,
            import_paths=self.import_paths,
            worker_defaults=self.worker_defaults,
        )
        app.tasks = self.tasks
        app.periodic_registry = self.periodic_registry
        return app

    @contextlib.contextmanager
    def replace_connector(
        self, connector: connector_module.BaseConnector
    ) -> Iterator[App]:
        """
        Replace the connector of the app while in the context block, then restore it.
        The context variable is the same app as this method is called on.

        >>> with app.replace_connector(new_connector) as app2:
        ...    ...
        ...    # app and app2 are the same object


        Parameters
        ----------
        connector :
            The new connector to use.

        Yields
        -------
        :
            A context manager that yields the same app with the new connector.
        """
        old_connector = self.connector
        self.connector = connector
        self.job_manager.connector = connector
        try:
            yield self
        finally:
            self.connector = old_connector
            self.job_manager.connector = old_connector

    def _register_builtin_tasks(self) -> None:
        from procrastinate import builtin_tasks

        self.add_tasks_from(builtin_tasks.builtin, namespace="builtin")

        # New tasks will be "builtin:procrastinate.builtin_tasks.remove_old_jobs"
        # but for compatibility, we can keep the old name around.
        self.add_task_alias(
            task=builtin_tasks.remove_old_jobs,
            alias="procrastinate.builtin_tasks.remove_old_jobs",
        )

    def will_configure_task(self) -> None:
        pass

    def configure_task(
        self, name: str, *, allow_unknown: bool = True, **kwargs: Any
    ) -> jobs.JobDeferrer:
        """
        Configure a task for deferring, using its name

        Parameters
        ----------
        name:
            Name of the task. If not explicitly defined, this will be the dotted path
            to the task (``my.module.my_task``)

        **kwargs: Any
            Parameters from `Task.configure`

        Returns
        -------
        :
            Launch ``.defer(**task_kwargs)`` on this object to defer your job.
        """
        from procrastinate import tasks

        self.perform_import_paths()
        try:
            return self.tasks[name].configure(**kwargs)
        except KeyError as exc:
            if allow_unknown:
                return tasks.configure_task(
                    name=name, job_manager=self.job_manager, **kwargs
                )
            raise exceptions.TaskNotFound from exc

    def _worker(self, **kwargs: Unpack[WorkerOptions]) -> worker.Worker:
        from procrastinate import worker

        final_kwargs: WorkerOptions = {**self.worker_defaults, **kwargs}

        return worker.Worker(app=self, **final_kwargs)

    @functools.lru_cache(maxsize=1)
    def perform_import_paths(self):
        """
        Whenever using app.tasks, make sure the apps have been imported by calling
        this method.
        """
        utils.import_all(import_paths=self.import_paths)
        logger.debug(
            "All tasks imported",
            extra={"action": "imported_tasks", "tasks": list(self.tasks)},
        )

    async def run_worker_async(self, **kwargs: Unpack[WorkerOptions]) -> None:
        """
        Run a worker. This worker will run in the foreground and execute the jobs in the
        provided queues. If wait is True, the function will not
        return until the worker stops (most probably when it receives a stop signal).
        The default values of all parameters presented here can be overridden at the
        `App` level.

        Parameters
        ----------
        queues: ``Optional[Iterable[str]]``
            List of queues to listen to, or None to listen to every queue (defaults to
            ``None``).
        wait: ``bool``
            If False, the worker will terminate as soon as it has caught up with the
            queues. If True, the worker will work until it is stopped by a signal
            (``ctrl+c``, ``SIGINT``, ``SIGTERM``) (defaults to ``True``).
        concurrency: ``int``
            Indicates how many asynchronous jobs the worker can run in parallel.
            Do not use concurrency if you have synchronous blocking tasks.
            See `howto/production/concurrency` (defaults to ``1``).
        name: ``Optional[str]``
            Name of the worker. Will be passed in the `JobContext` and used in the
            logs (defaults to ``None`` which will result in the worker named
            ``worker``).
        fetch_job_polling_interval : ``float``
            Maximum time (in seconds) between database job polls.

            Controls the frequency of database queries for new jobs to start.

            When `listen_notify` is True, the polling interval acts as a fallback
            mechanism and can reasonably be set to a higher value.

            (defaults to 5.0)
        abort_job_polling_interval : ``float``
            Maximum time (in seconds) between database abort requet polls.

            Controls the frequency of database queries for abort requests

            When `listen_notify` is True, the polling interval acts as a fallback
            mechanism and can reasonably be set to a higher value.

            (defaults to 5.0)
        shutdown_graceful_timeout: ``float``
            Indicates the maximum duration (in seconds) the worker waits for jobs to
            complete when requested to stop. Jobs that have not been completed by that time
            are aborted. A value of None corresponds to no timeout.

            (defaults to None)
        listen_notify : ``bool``
            If ``True``, allocates a connection from the pool to
            listen for:
            - new job availability
            - job abort requests

            Provides lower latency for job updates compared to polling alone.

            Note: Worker polls the database regardless of this setting. (defaults to ``True``)
        delete_jobs : ``str``
            If ``always``, the worker will automatically delete all jobs on completion.
            If ``successful`` the worker will only delete successful jobs.
            If ``never``, the worker will keep the jobs in the database.
            (defaults to ``never``)
        additional_context: ``Optional[Dict[str, Any]]``
            If set extend the context received by the tasks when ``pass_context`` is set
            to ``True`` in the task definition.
        install_signal_handlers: ``bool``
            If ``True``, the worker will install signal handlers to gracefully stop the
            worker. Use ``False`` if you want to handle signals yourself (e.g. if you
            run the work as an async task in a bigger application)
            (defaults to ``True``)
        update_heartbeat_interval: ``float``
            Time in seconds between heartbeat updates of the worker. (defaults to 10)
        stalled_worker_timeout: ``float``
            Time in seconds after which a worker is considered stalled if no heartbeat has
            been received. A worker prunes stalled workers from the database at startup.
            (defaults to 30)
        """
        self.perform_import_paths()
        worker = self._worker(**kwargs)
        await worker.run()

    def run_worker(self, **kwargs: Any) -> None:
        """
        Synchronous version of `App.run_worker_async`.
        Create the event loop and open the app, then run the worker.
        The app and the event loop are closed at the end of the function.
        """

        async def f():
            async with self.open_async():
                await self.run_worker_async(**kwargs)

        asyncio.run(f())

    async def check_connection_async(self) -> bool:
        return await self.job_manager.check_connection_async()

    def check_connection(self) -> bool:
        return self.job_manager.check_connection()

    @property
    def schema_manager(self) -> schema.SchemaManager:
        return schema.SchemaManager(connector=self.connector)

    def open(
        self,
        pool_or_engine: connector_module.Pool | connector_module.Engine | None = None,
    ) -> App:
        """
        Open the app synchronously.

        Parameters
        ----------
        pool_or_engine :
            Optional pool. Procrastinate can use an existing pool.
            Connection parameters passed in the constructor will be ignored.
            In case the SQLAlchemy connector is used, this can be an engine.
        """
        self.connector.get_sync_connector().open(pool_or_engine)
        return self

    def close(self) -> None:
        self.connector.get_sync_connector().close()

    def open_async(
        self, pool: connector_module.Pool | None = None
    ) -> utils.AwaitableContext:
        """
        Open the app asynchronously.

        Parameters
        ----------
        pool :
            Optional pool. Procrastinate can use an existing pool. Connection parameters
            passed in the constructor will be ignored.
        """
        open_coro = functools.partial(self.connector.open_async, pool=pool)
        return utils.AwaitableContext(
            open_coro=open_coro,
            close_coro=self.connector.close_async,
            return_value=self,
        )

    async def close_async(self) -> None:
        await self.connector.close_async()

    def __enter__(self) -> App:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
