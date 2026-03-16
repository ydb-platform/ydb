import logging

from asgiref.local import Local
from django.core.signals import setting_changed
from django.dispatch import Signal, receiver

from django_tasks import BaseTaskBackend, TaskResult, TaskResultStatus

task_enqueued = Signal()
task_finished = Signal()
task_started = Signal()


logger = logging.getLogger("django_tasks")


@receiver(setting_changed)
def clear_tasks_handlers(*, setting: str, **kwargs: dict) -> None:
    """
    Reset the connection handler whenever the settings change.
    """
    if setting == "TASKS":
        from django_tasks import task_backends

        task_backends._settings = task_backends.settings = (  # type:ignore[attr-defined]
            task_backends.configure_settings(None)
        )
        task_backends._connections = Local()  # type:ignore[attr-defined]


@receiver(task_enqueued)
def log_task_enqueued(
    sender: type[BaseTaskBackend], task_result: TaskResult, **kwargs: dict
) -> None:
    logger.debug(
        "Task id=%s path=%s enqueued backend=%s",
        task_result.id,
        task_result.task.module_path,
        task_result.backend,
    )


@receiver(task_started)
def log_task_started(
    sender: type[BaseTaskBackend], task_result: TaskResult, **kwargs: dict
) -> None:
    logger.info(
        "Task id=%s path=%s state=%s",
        task_result.id,
        task_result.task.module_path,
        task_result.status,
    )


@receiver(task_finished)
def log_task_finished(
    sender: type[BaseTaskBackend], task_result: TaskResult, **kwargs: dict
) -> None:
    if task_result.status == TaskResultStatus.FAILED:
        # Use exception to integrate with error monitoring tools
        log_method = logger.exception
    else:
        log_method = logger.info

    log_method(
        "Task id=%s path=%s state=%s",
        task_result.id,
        task_result.task.module_path,
        task_result.status,
    )
