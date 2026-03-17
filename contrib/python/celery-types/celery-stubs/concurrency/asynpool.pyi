from typing import NamedTuple

from billiard import pool as _pool
from typing_extensions import override

__all__ = ["AsynPool"]

class Ack(NamedTuple): ...
class Worker(_pool.Worker): ...

class ResultHandler(_pool.ResultHandler):
    @override
    def on_stop_not_started(self) -> None: ...

class AsynPool(_pool.Pool):
    def flush(self) -> None: ...
