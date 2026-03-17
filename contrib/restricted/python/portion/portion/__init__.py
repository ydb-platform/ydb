import importlib.metadata

from .api import create_api
from .const import Bound, inf
from .dict import IntervalDict
from .func import closed, closedopen, empty, iterate, open, openclosed, singleton
from .interval import AbstractDiscreteInterval, Interval
from .io import from_data, from_string, to_data, to_string

__all__ = [
    "create_api",
    "inf",
    "CLOSED",
    "OPEN",
    "Interval",
    "AbstractDiscreteInterval",
    "open",
    "closed",
    "openclosed",
    "closedopen",
    "singleton",
    "empty",
    "iterate",
    "from_string",
    "to_string",
    "from_data",
    "to_data",
    "IntervalDict",
]

CLOSED = Bound.CLOSED
OPEN = Bound.OPEN

__version__ = importlib.metadata.version("portion")
