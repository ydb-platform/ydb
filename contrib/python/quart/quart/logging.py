from __future__ import annotations

import sys
from logging import (
    DEBUG,
    Formatter,
    getLogger,
    Handler,
    INFO,
    Logger,
    LogRecord,
    NOTSET,
    StreamHandler,
)
from logging.handlers import QueueHandler, QueueListener
from queue import SimpleQueue as Queue
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .app import Quart  # noqa

default_handler = StreamHandler(sys.stderr)
default_handler.setFormatter(Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s"))

serving_handler = StreamHandler(sys.stdout)
serving_handler.setFormatter(Formatter("[%(asctime)s] %(message)s"))


class LocalQueueHandler(QueueHandler):
    """Custom QueueHandler that skips record preparation.

    There is no need to prepare records that go into a local, in-process queue,
    we can skip that process and minimise the cost of logging further.
    """

    def prepare(self, record: LogRecord) -> LogRecord:
        return record


def _setup_logging_queue(*handlers: Handler) -> QueueHandler:
    """Create a new LocalQueueHandler and start an associated QueueListener."""
    queue: Queue = Queue()
    queue_handler = LocalQueueHandler(queue)

    serving_listener = QueueListener(queue, *handlers, respect_handler_level=True)
    serving_listener.start()

    return queue_handler


def create_logger(app: "Quart") -> Logger:
    """Create a logger for the app based on the app settings.

    This creates a logger named quart.app that has a log level based
    on the app configuration.
    """
    logger = getLogger("quart.app")

    if app.debug and logger.level == NOTSET:
        logger.setLevel(DEBUG)

    queue_handler = _setup_logging_queue(default_handler)
    logger.addHandler(queue_handler)
    return logger


def create_serving_logger() -> Logger:
    """Create a logger for serving.

    This creates a logger named quart.serving.
    """
    logger = getLogger("quart.serving")

    if logger.level == NOTSET:
        logger.setLevel(INFO)

    queue_handler = _setup_logging_queue(serving_handler)
    logger.addHandler(queue_handler)
    return logger
