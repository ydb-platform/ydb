# Standard library imports
import sys
import time

# Local imports
from uplink.clients.io import interfaces

__all__ = ["BlockingStrategy"]


class BlockingStrategy(interfaces.IOStrategy):
    """A blocking execution strategy."""

    def invoke(self, func, arg, kwargs, callback):
        try:
            response = func(*arg, **kwargs)
        except Exception as error:
            tb = sys.exc_info()[2]
            return callback.on_failure(type(error), error, tb)
        else:
            return callback.on_success(response)

    def sleep(self, duration, callback):
        time.sleep(duration)
        return callback.on_success()

    def finish(self, response):
        return response

    def execute(self, executable):
        return executable.execute()
