import sys
from contextlib import contextmanager
from typing import Generator

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import BaseExceptionGroup


@contextmanager
def collapse_excgroups() -> Generator[None, None, None]:
    try:
        yield
    except BaseException as exc:
        while (
            isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1
        ):  # pragma: nopy38
            exc = exc.exceptions[0]

        raise exc
