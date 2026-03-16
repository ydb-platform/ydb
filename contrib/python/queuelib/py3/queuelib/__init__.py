__version__ = "1.9.0"

from queuelib.pqueue import PriorityQueue
from queuelib.queue import FifoDiskQueue, LifoDiskQueue
from queuelib.rrqueue import RoundRobinQueue

__all__ = [
    "FifoDiskQueue",
    "LifoDiskQueue",
    "PriorityQueue",
    "RoundRobinQueue",
]
