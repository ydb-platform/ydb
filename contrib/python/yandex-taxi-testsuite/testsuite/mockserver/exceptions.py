from testsuite.utils import traceback


class BaseError(Exception):
    """Base class for exceptions from this package."""


class MockServerError(BaseError):
    pass


class HandlerNotFoundError(MockServerError):
    pass


__tracebackhide__ = traceback.hide(BaseError)
