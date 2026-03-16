import sys
from contextlib import contextmanager
from typing import Generator

# AnyIO v4 introduces a breaking change that groups all exceptions in a task
# group into an exception group.
# This file allows to be compatible with AnyIO <4 and >=4 by unwrapping groups
# if they only contain a single exception. It also supports python <3.11 (before
# exception groups support) and >=3.11.
# Solution as proposed in https://anyio.readthedocs.io/en/stable/migration.html

has_exceptiongroups = True
if sys.version_info < (3, 11):
    try:
        from exceptiongroup import BaseExceptionGroup
    except ImportError:
        has_exceptiongroups = False


@contextmanager
def collapse_excgroups() -> Generator[None, None, None]:
    try:
        yield
    except BaseException as exc:
        if has_exceptiongroups:
            while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
                exc = exc.exceptions[0]

        raise exc


__all__ = ["collapse_excgroups"]
