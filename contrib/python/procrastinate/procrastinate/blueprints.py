from __future__ import annotations

import functools
import logging
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Concatenate, Literal, cast, overload

from typing_extensions import ParamSpec, TypeVar, Unpack

from procrastinate import exceptions, jobs, periodic, retry, utils
from procrastinate.job_context import JobContext

if TYPE_CHECKING:
    from procrastinate.tasks import ConfigureTaskOptions, Task


logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


class Blueprint:
    """
    A Blueprint provides a way to declare tasks that can be registered on an
    `App` later::

        # Create blueprint for all tasks related to the cat
        cat_blueprint = Blueprint()

        # Declare tasks
        @cat_blueprint.task(lock="...")
        def feed_cat():
            ...

        # Register blueprint (will register ``cat:path.to.feed_cat``)
        app.add_tasks_from(cat_blueprint, namespace="cat")

    A blueprint can add tasks from another blueprint::

        blueprint_a, blueprint_b = Blueprint(), Blueprint()

        @blueprint_b.task(lock="...")
        def my_task():
            ...

        blueprint_a.add_tasks_from(blueprint_b, namespace="b")

        # Registers task "a:b:path.to.my_task"
        app.add_tasks_from(blueprint_a, namespace="a")

    Raises
    ------
    UnboundTaskError:
        Calling a blueprint task before the it is bound to an `App` will raise a
        `UnboundTaskError` error::

            blueprint = Blueprint()

            # Declare tasks
            @blueprint.task
            def my_task():
                ...

            >>> my_task.defer()

            Traceback (most recent call last):
                File "..."
            `UnboundTaskError`: ...
    """

    def __init__(self) -> None:
        #: The mapping of all tasks known by the app. Only procrastinate is
        #: expected to make changes to this mapping.
        self.tasks: dict[str, Task] = {}
        self.periodic_registry = periodic.PeriodicRegistry()
        self._check_stack()

    def _check_stack(self):
        # Emit a warning if the app is defined in the __main__ module
        name = None
        try:
            name = utils.caller_module_name()
        except exceptions.CallerModuleUnknown:
            logger.warning(
                "Unable to determine where the app was defined. "
                "See https://procrastinate.readthedocs.io/en/stable/discussions.html#top-level-app",
                extra={"action": "app_location_unknown"},
                exc_info=True,
            )

        if name == "__main__":
            logger.warning(
                f"{type(self).__name__} is instantiated in the main Python module "
                f"({sys.argv[0]}). "
                "See https://procrastinate.readthedocs.io/en/stable/discussions.html#top-level-app",
                extra={"action": "app_defined_in___main__"},
            )

    def _register_task(self, task: Task) -> None:
        """
        Register the task into the blueprint task registry.
        Raises exceptions.TaskAlreadyRegistered if the task name
        or an alias already exists in the registry
        """

        # Each call to _add_task may raise TaskAlreadyRegistered.
        # We're using an intermediary dict to make sure that if the registration
        # is interrupted midway though, self.tasks is left unmodified.
        to_add: dict[str, Task] = {}
        self._add_task(task=task, name=task.name, to=to_add)

        for alias in task.aliases:
            self._add_task(task=task, name=alias, to=to_add)

        self.tasks.update(to_add)

    def _add_task(self, task: Task, name: str, to: dict | None = None) -> None:
        # Add a task to a dict of task while making
        # sure a task of the same name was not already in self.tasks.
        # This lets us prepare a dict of tasks we might add while not adding
        # them until we're 100% sure there's no clash.

        if name in self.tasks:
            raise exceptions.TaskAlreadyRegistered(
                f"A task named {name} was already registered"
            )

        result_dict = self.tasks if to is None else to
        result_dict[name] = task

    def add_task_alias(self, task: Task, alias: str) -> None:
        """
        Add an alias to a task. This can be useful if a task was in a given
        Blueprint and moves to a different blueprint.

        Parameters
        ----------
        task :
            Task to alias
        alias :
            New alias (including potential namespace, separated with ``:``)
        """
        self._add_task(task=task, name=alias)

    def add_tasks_from(self, blueprint: Blueprint, *, namespace: str) -> None:
        """
        Copies over all tasks from a different blueprint, prefixing their names
        with the given namespace (using ``:`` as namespace separator).

        Parameters
        ----------
        blueprint :
            Blueprint to copy tasks from
        namespace :
            All task names (but not aliases) will be prefixed by this name,
            uniqueness will be enforced.

        Raises
        ------
        TaskAlreadyRegistered:
            When trying to use a namespace that has already been used before
        """
        # Compute new task names
        new_tasks = {
            utils.add_namespace(name=name, namespace=namespace): task
            for name, task in blueprint.tasks.items()
        }
        if set(self.tasks) & set(new_tasks):
            raise exceptions.TaskAlreadyRegistered(
                f"A namespace named {namespace} was already registered"
            )
        # Modify existing tasks and other blueprint to add the namespace, and
        # set the blueprint
        for task in set(blueprint.tasks.values()):
            task.add_namespace(namespace)
            task.blueprint = self
        blueprint.tasks = new_tasks

        logger.info(
            "Adding tasks from blueprint",
            extra={
                "action": "loading_blueprint",
                "namespace": namespace,
                "tasks": [t.name for t in new_tasks.values()],
            },
        )

        # Add the namespaced tasks to this namespace
        self.tasks.update(new_tasks)

        # Add periodic tasks
        for periodic_task in blueprint.periodic_registry.periodic_tasks.values():
            self.periodic_registry.register_task(
                task=periodic_task.task,
                cron=periodic_task.cron,
                periodic_id=periodic_task.periodic_id,
                configure_kwargs=periodic_task.configure_kwargs,
            )

    @overload
    def task(
        self,
        *,
        _func: None = None,
        name: str | None = None,
        aliases: list[str] | None = None,
        retry: retry.RetryValue = False,
        pass_context: Literal[False] = False,
        queue: str = jobs.DEFAULT_QUEUE,
        priority: int = jobs.DEFAULT_PRIORITY,
        lock: str | None = None,
        queueing_lock: str | None = None,
    ) -> Callable[[Callable[P, R]], Task[P, R, P]]:
        """Declare a function as a task. This method is meant to be used as a decorator
        Parameters
        ----------
        queue :
            The name of the queue in which jobs from this task will be launched, if
            the queue is not overridden at launch.
            Default is ``"default"``.
            When a worker is launched, it can listen to specific queues, or to all
            queues.
        lock :
            Default value for the ``lock`` (see `Task.defer`).
        queueing_lock:
            Default value for the ``queueing_lock`` (see `Task.defer`).
        name :
            Name of the task, by default the full dotted path to the decorated function.
            if the function is nested or dynamically defined, it is important to give
            it a unique name, and to make sure the module that defines this function
            is listed in the ``import_paths`` of the `procrastinate.App`.
        priority :
            Default priority (an integer) of jobs that are deferred from this task.
            Jobs with higher priority are run first. Priority can be positive or negative.
            If no default priority is set then the default priority is 0.
        aliases:
            Additional names for the task.
            The main use case is to gracefully rename tasks by moving the old
            name to aliases (these tasks can have been scheduled in a distant
            future, so the aliases can remain for a long time).
        retry :
            Details how to auto-retry the task if it fails. Can be:

            - A ``boolean``: will either not retry or retry indefinitely
            - An ``int``: the number of retries before it gives up
            - A `procrastinate.RetryStrategy` instance for complex cases

            Default is no retry.
        pass_context :
            Passes the task execution context in the task as first
        """
        ...

    @overload
    def task(
        self,
        *,
        _func: None = None,
        name: str | None = None,
        aliases: list[str] | None = None,
        retry: retry.RetryValue = False,
        pass_context: Literal[True],
        queue: str = jobs.DEFAULT_QUEUE,
        priority: int = jobs.DEFAULT_PRIORITY,
        lock: str | None = None,
        queueing_lock: str | None = None,
    ) -> Callable[
        [Callable[Concatenate[JobContext, P], R]],
        Task[Concatenate[JobContext, P], R, P],
    ]:
        """Declare a function as a task. This method is meant to be used as a decorator
        Parameters
        ----------
        _func :
            The decorated function
        """
        ...

    @overload
    def task(self, _func: Callable[P, R]) -> Task[P, R, P]:
        """Declare a function as a task. This method is meant to be used as a decorator
        Parameters
        ----------
        _func :
            The decorated function
        """
        ...

    def task(
        self,
        _func: Callable[P, R] | None = None,
        *,
        name: str | None = None,
        aliases: list[str] | None = None,
        retry: retry.RetryValue = False,
        pass_context: bool = False,
        queue: str = jobs.DEFAULT_QUEUE,
        priority: int = jobs.DEFAULT_PRIORITY,
        lock: str | None = None,
        queueing_lock: str | None = None,
    ):
        from procrastinate.tasks import Task

        """
        Declare a function as a task. This method is meant to be used as a decorator::

            @app.task(...)
            def my_task(args):
                ...

        or::

            @app.task
            def my_task(args):
                ...
        The second form will use the default value for all parameters.
        """

        def _wrap(func: Callable[P, R]) -> Task[P, R, P]:
            task = Task(
                func,
                blueprint=self,
                queue=queue,
                priority=priority,
                lock=lock,
                queueing_lock=queueing_lock,
                name=name,
                aliases=aliases,
                retry=retry,
                pass_context=pass_context,
            )
            self._register_task(task)

            # The signature of a function returned by functools.update_wrapper
            # is the same as the signature of the wrapped function (at least on pyright).
            # Here, we're actually returning a Task so a cast is needed to provide the correct signature.
            return cast(  # pyright: ignore[reportInvalidCast]
                Task[P, R, P],
                functools.update_wrapper(task, func, updated=()),
            )

        if _func is None:  # Called as @app.task(...)
            return cast(
                Callable[[Callable[P, R]], Task[P, R, P]]
                | Callable[
                    [Callable[Concatenate[JobContext, P], R]],
                    Task[Concatenate[JobContext, P], R, P],
                ],
                _wrap,
            )

        return _wrap(_func)  # Called as @app.task

    def periodic(
        self,
        *,
        cron: str,
        periodic_id: str = "",
        **configure_kwargs: Unpack[ConfigureTaskOptions],
    ):
        """
        Task decorator, marks task as being scheduled for periodic deferring (see
        `howto/advanced/cron`).

        Parameters
        ----------
        cron :
            Cron-like string. Optionally add a 6th column for seconds.
        periodic_id :
            Task name suffix. Used to distinguish periodic tasks with different kwargs.
        **kwargs :
            Additional parameters are passed to `Task.configure`.
        """
        return self.periodic_registry.periodic_decorator(
            cron=cron, periodic_id=periodic_id, **configure_kwargs
        )

    def will_configure_task(self) -> None:
        raise exceptions.UnboundTaskError
