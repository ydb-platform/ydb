from celery import current_task, signals
import logging as _logging

from ..request_id import current_request_id
from ..ctx_fetcher import ExecutedOutsideContext


_CELERY_X_HEADER = 'x_request_id'
logger = _logging.getLogger(__name__)


def enable_request_id_propagation(celery_app):
    """
    Will attach signal on celery application in order to propagate
    current request id to workers
    :param celery_app: The celery application
    """
    signals.before_task_publish.connect(on_before_publish_insert_request_id_header)


def on_before_publish_insert_request_id_header(headers, **kwargs):
    """
    This function is meant to be used as signal processor for "before_task_publish".
    :param Dict headers: The headers of the message
    :param kwargs: Any extra keyword arguments
    """
    if _CELERY_X_HEADER not in headers:
        request_id = current_request_id()
        headers[_CELERY_X_HEADER] = request_id
        logger.debug("Forwarding request_id '{}' to the task consumer.".format(request_id))


def ctx_celery_task_get_request_id():
    """
    Fetch the request id from the headers of the current celery task.
    """
    if current_task._get_current_object() is None:
        raise ExecutedOutsideContext()

    return current_task.request.get(_CELERY_X_HEADER, None)


# If you import this module then you are interested for this context
current_request_id.register_fetcher(ctx_celery_task_get_request_id)
