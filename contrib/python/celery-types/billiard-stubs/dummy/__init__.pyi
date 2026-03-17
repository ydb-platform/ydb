import threading
from queue import Queue as Queue
from threading import BoundedSemaphore as BoundedSemaphore
from threading import Event as Event
from threading import Lock as Lock
from threading import RLock as RLock
from threading import Semaphore as Semaphore

from typing_extensions import override

__all__ = [
    "BoundedSemaphore",
    "Event",
    "JoinableQueue",
    "Lock",
    "Process",
    "Queue",
    "RLock",
    "Semaphore",
    "current_process",
    "freeze_support",
]

class DummyProcess(threading.Thread):
    def __init__(
        self,
    ) -> None: ...
    @override
    def start(self) -> None: ...

Process = DummyProcess
current_process = threading.current_thread

def freeze_support() -> None: ...

JoinableQueue = Queue
