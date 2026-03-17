from django.core.exceptions import ImproperlyConfigured


class TaskException(Exception):  # noqa: N818
    """Base class for task-related exceptions. Do not raise directly."""


class InvalidTaskError(TaskException):
    """
    The provided Task is invalid.
    """


class InvalidTaskBackendError(ImproperlyConfigured):
    pass


class TaskResultDoesNotExist(TaskException):
    pass


class TaskResultMismatch(TaskException):
    pass
