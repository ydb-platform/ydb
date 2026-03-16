from .log import logger

__all__ = (
    'CancelledError', 'TimeoutError',
    'FIRST_COMPLETED', 'FIRST_EXCEPTION', 'ALL_COMPLETED',
    )

# Argument for default thread pool executor creation.
_MAX_WORKERS = 5

try:
    import concurrent.futures
    import concurrent.futures._base
except ImportError:
    FIRST_COMPLETED = 'FIRST_COMPLETED'
    FIRST_EXCEPTION = 'FIRST_EXCEPTION'
    ALL_COMPLETED = 'ALL_COMPLETED'

    class Future(object):
        def __init__(self, callback, args):
            try:
                self._result = callback(*args)
                self._exception = None
            except Exception as err:
                self._result = None
                self._exception = err
            self.callbacks = []

        def cancelled(self):
            return False

        def done(self):
            return True

        def exception(self):
            return self._exception

        def result(self):
            if self._exception is not None:
                raise self._exception
            else:
                return self._result

        def add_done_callback(self, callback):
            callback(self)

    class Error(Exception):
        """Base class for all future-related exceptions."""
        pass

    class CancelledError(Error):
        """The Future was cancelled."""
        pass

    class TimeoutError(Error):
        """The operation exceeded the given deadline."""
        pass

    class SynchronousExecutor:
        """
        Synchronous executor: submit() blocks until it gets the result.
        """
        def submit(self, callback, *args):
            return Future(callback, args)

        def shutdown(self, wait):
            pass

    def get_default_executor():
        logger.error("concurrent.futures module is missing: "
                     "use a synchrounous executor as fallback!")
        return SynchronousExecutor()
else:
    FIRST_COMPLETED = concurrent.futures.FIRST_COMPLETED
    FIRST_EXCEPTION = concurrent.futures.FIRST_EXCEPTION
    ALL_COMPLETED = concurrent.futures.ALL_COMPLETED

    Future = concurrent.futures.Future
    Error = concurrent.futures._base.Error
    CancelledError = concurrent.futures.CancelledError
    TimeoutError = concurrent.futures.TimeoutError

    def get_default_executor():
        return concurrent.futures.ThreadPoolExecutor(_MAX_WORKERS)
