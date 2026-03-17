import logging
import math
import os
import random
import signal
import sys
import time
from argparse import ArgumentParser, ArgumentTypeError, BooleanOptionalAction
from types import FrameType

from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.db.utils import OperationalError
from django.utils.autoreload import DJANGO_AUTORELOAD_ENV, run_with_reloader

from django_tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django_tasks.backends.database.backend import DatabaseBackend
from django_tasks.backends.database.models import DBTaskResult
from django_tasks.backends.database.utils import exclusive_transaction
from django_tasks.base import DEFAULT_TASK_QUEUE_NAME, TaskContext
from django_tasks.exceptions import InvalidTaskBackendError
from django_tasks.signals import task_finished, task_started
from django_tasks.utils import get_random_id

package_logger = logging.getLogger("django_tasks")
logger = logging.getLogger("django_tasks.backends.database.db_worker")


class Worker:
    def __init__(
        self,
        *,
        queue_names: list[str],
        interval: float,
        batch: bool,
        backend_name: str,
        startup_delay: bool,
        max_tasks: int | None,
        worker_id: str,
    ):
        self.queue_names = queue_names
        self.process_all_queues = "*" in queue_names
        self.interval = interval
        self.batch = batch
        self.backend_name = backend_name
        self.startup_delay = startup_delay
        self.max_tasks = max_tasks

        self.running = True
        self.running_task = False
        self._run_tasks = 0

        self.worker_id = worker_id

    def shutdown(self, signum: int, frame: FrameType | None) -> None:
        if not self.running:
            logger.warning(
                "Received %s - terminating current task.", signal.strsignal(signum)
            )
            self.reset_signals()
            sys.exit(1)

        logger.warning(
            "Received %s - shutting down gracefully... (press Ctrl+C again to force)",
            signal.strsignal(signum),
        )
        self.running = False

        if not self.running_task:
            # If we're not currently running a task, exit immediately.
            # This is useful if we're currently in a `sleep`.
            sys.exit(0)

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.shutdown)

    def reset_signals(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, signal.SIG_DFL)

    def run(self) -> None:
        logger.info(
            "Starting worker worker_id=%s queues=%s",
            self.worker_id,
            ",".join(self.queue_names),
        )

        if self.startup_delay and self.interval:
            # Add a random small delay before starting to avoid a thundering herd
            time.sleep(random.random())  # noqa: S311

        while self.running:
            tasks = DBTaskResult.objects.ready().filter(backend_name=self.backend_name)
            if not self.process_all_queues:
                tasks = tasks.filter(queue_name__in=self.queue_names)

            # During this transaction, all "ready" tasks are locked. Therefore, it's important
            # it be as efficient as possible.
            with exclusive_transaction(tasks.db):
                try:
                    task_result = tasks.get_locked()
                except OperationalError as e:
                    # Ignore locked databases and keep trying.
                    # It should unlock eventually.
                    if "is locked" in e.args[0]:
                        task_result = None
                    else:
                        raise

                if task_result is not None:
                    # "claim" the task, so it isn't run by another worker process
                    task_result.claim(self.worker_id)

            if task_result is not None:
                self.run_task(task_result)

            if self.batch and task_result is None:
                # If we're running in "batch" mode, terminate the loop (and thus the worker)
                logger.info(
                    "No more tasks to run for worker_id=%s - exiting gracefully.",
                    self.worker_id,
                )
                return None

            if self.max_tasks is not None and self._run_tasks >= self.max_tasks:
                logger.info(
                    "Run maximum tasks (%d) on worker=%s - exiting gracefully.",
                    self._run_tasks,
                    self.worker_id,
                )
                return None

            # Emulate Django's request behaviour and check for expired
            # database connections periodically.
            close_old_connections()

            # If ctrl-c has just interrupted a task, self.running was cleared,
            # and we should not sleep, but rather exit immediately.
            if self.running and not task_result:
                # Wait before checking for another task
                time.sleep(self.interval)

    def run_task(self, db_task_result: DBTaskResult) -> None:
        """
        Run the given task, marking it as succeeded or failed.
        """
        try:
            self.running_task = True
            task = db_task_result.task
            task_result = db_task_result.task_result

            backend_type = task.get_backend()

            task_started.send(sender=backend_type, task_result=task_result)
            if task.takes_context:
                return_value = task.call(
                    TaskContext(task_result=task_result),
                    *task_result.args,
                    **task_result.kwargs,
                )
            else:
                return_value = task.call(*task_result.args, **task_result.kwargs)

            # Setting the return and success value inside the error handling,
            # So errors setting it (eg JSON encode) can still be recorded
            db_task_result.set_succeeded(return_value, task_result.metadata)
            task_finished.send(
                sender=backend_type, task_result=db_task_result.task_result
            )
        except BaseException as e:
            try:
                metadata = task_result.metadata
            except NameError:
                metadata = None

            db_task_result.set_failed(e, metadata)

            try:
                sender = type(db_task_result.task.get_backend())
                task_result = db_task_result.task_result
            except (ModuleNotFoundError, SuspiciousOperation):
                logger.exception("Task id=%s failed unexpectedly", db_task_result.id)
            else:
                task_finished.send(
                    sender=sender,
                    task_result=task_result,
                )
        finally:
            self.running_task = False
            self._run_tasks += 1


def valid_backend_name(val: str) -> str:
    try:
        backend = task_backends[val]
    except InvalidTaskBackendError as e:
        raise ArgumentTypeError(e.args[0]) from e
    if not isinstance(backend, DatabaseBackend):
        raise ArgumentTypeError(f"Backend '{val}' is not a database backend")
    return val


def valid_interval(val: str) -> float:
    num = float(val)
    if not math.isfinite(num):
        raise ArgumentTypeError("Must be a finite floating point value")
    if num < 0:
        raise ArgumentTypeError("Must be greater than zero")
    return num


def valid_max_tasks(val: str) -> int:
    num = int(val)
    if num < 0:
        raise ArgumentTypeError("Must be greater than zero")
    return num


def validate_worker_id(val: str) -> str:
    if not val:
        raise ArgumentTypeError("Worker id must not be empty")
    if len(val) > 64:
        raise ArgumentTypeError("Worker ids must be shorter than 64 characters")
    return val


class Command(BaseCommand):
    help = "Run a database background worker"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--queue-name",
            nargs="?",
            default=DEFAULT_TASK_QUEUE_NAME,
            type=str,
            help="The queues to process. Separate multiple with a comma. To process all queues, use '*' (default: %(default)r)",
        )
        parser.add_argument(
            "--interval",
            nargs="?",
            default=1,
            type=valid_interval,
            help="The interval (in seconds) to wait, when there are no tasks in the queue, before checking for tasks again (default: %(default)r)",
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Process all outstanding tasks, then exit. Can be used in combination with --max-tasks.",
        )
        parser.add_argument(
            "--reload",
            action=BooleanOptionalAction,
            default=settings.DEBUG,
            help="Reload the worker on code changes. Not recommended for production as tasks may not be stopped cleanly (default: DEBUG)",
        )
        parser.add_argument(
            "--backend",
            nargs="?",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            type=valid_backend_name,
            dest="backend_name",
            help="The backend to operate on (default: %(default)r)",
        )
        parser.add_argument(
            "--no-startup-delay",
            action="store_false",
            dest="startup_delay",
            help="Don't add a small delay at startup.",
        )
        parser.add_argument(
            "--max-tasks",
            nargs="?",
            default=None,
            type=valid_max_tasks,
            help="If provided, the maximum number of tasks the worker will execute before exiting.",
        )
        parser.add_argument(
            "--worker-id",
            nargs="?",
            type=validate_worker_id,
            help="Worker id. MUST be unique across worker pool (default: auto-generate)",
            default=get_random_id(),
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            package_logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            package_logger.setLevel(logging.INFO)
        else:
            package_logger.setLevel(logging.DEBUG)

        # If no handler is configured, the logs won't show,
        # regardless of the set level.
        if not package_logger.hasHandlers():
            package_logger.addHandler(logging.StreamHandler(self.stdout))

    def handle(
        self,
        *,
        verbosity: int,
        queue_name: str,
        interval: float,
        batch: bool,
        backend_name: str,
        startup_delay: bool,
        reload: bool,
        max_tasks: int | None,
        worker_id: str,
        **options: dict,
    ) -> None:
        self.configure_logging(verbosity)

        if reload and batch:
            logger.warning(
                "Warning: --reload and --batch cannot be specified together. Disabling autoreload."
            )
            reload = False

        worker = Worker(
            queue_names=queue_name.split(","),
            interval=interval,
            batch=batch,
            backend_name=backend_name,
            startup_delay=startup_delay,
            max_tasks=max_tasks,
            worker_id=worker_id,
        )

        if reload:
            if os.environ.get(DJANGO_AUTORELOAD_ENV) == "true":
                # Only the child process should configure its signals
                worker.configure_signals()

            run_with_reloader(worker.run)
        else:
            worker.configure_signals()
            worker.run()
