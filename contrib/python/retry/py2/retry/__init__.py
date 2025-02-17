__all__ = ['retry', 'retry_call']

import logging

from .api import retry, retry_call


# Set default logging handler to avoid "No handler found" warnings.
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):

        def emit(self, record):
            pass

log = logging.getLogger(__name__)
log.addHandler(NullHandler())
