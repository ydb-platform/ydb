from __future__ import absolute_import


class ThreadLoopException(Exception):
    """Top-level library exception."""
    pass


class ThreadNotStartedError(ThreadLoopException):
    """Raised when calling submit before ThreadLoop.start() was called."""
    pass
