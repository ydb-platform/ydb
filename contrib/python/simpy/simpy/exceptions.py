"""
SimPy specific exceptions.

"""
from __future__ import annotations

from typing import Any, Optional


class SimPyException(Exception):
    """Base class for all SimPy specific exceptions."""


class Interrupt(SimPyException):
    """Exception thrown into a process if it is interrupted (see
    :func:`~simpy.events.Process.interrupt()`).

    :attr:`cause` provides the reason for the interrupt, if any.

    If a process is interrupted concurrently, all interrupts will be thrown
    into the process in the same order as they occurred.


    """

    def __init__(self, cause: Optional[Any]):
        super().__init__(cause)

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.cause!r})'

    @property
    def cause(self) -> Optional[Any]:
        """The cause of the interrupt or ``None`` if no cause was provided."""
        return self.args[0]
