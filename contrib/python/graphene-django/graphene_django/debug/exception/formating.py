import traceback

from django.utils.encoding import force_str

from .types import DjangoDebugException


def wrap_exception(exception):
    return DjangoDebugException(
        message=force_str(exception),
        exc_type=force_str(type(exception)),
        stack="".join(
            traceback.format_exception(
                exception, value=exception, tb=exception.__traceback__
            )
        ),
    )
