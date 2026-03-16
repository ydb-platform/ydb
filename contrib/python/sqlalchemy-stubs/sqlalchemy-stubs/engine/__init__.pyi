from typing import Any
from . import default as default

from .interfaces import (
    Connectable as Connectable,
    CreateEnginePlugin as CreateEnginePlugin,
    Dialect as Dialect,
    ExecutionContext as ExecutionContext,
    ExceptionContext as ExceptionContext,
    Compiled as Compiled,
    TypeCompiler as TypeCompiler,
)

from .base import (
    Connection as Connection,
    Engine as Engine,
    NestedTransaction as NestedTransaction,
    RootTransaction as RootTransaction,
    Transaction as Transaction,
    TwoPhaseTransaction as TwoPhaseTransaction,
)

from .result import (
    BaseRowProxy as BaseRowProxy,
    BufferedColumnResultProxy as BufferedColumnResultProxy,
    BufferedColumnRow as BufferedColumnRow,
    BufferedRowResultProxy as BufferedRowResultProxy,
    FullyBufferedResultProxy as FullyBufferedResultProxy,
    ResultProxy as ResultProxy,
    RowProxy as RowProxy,
)

def create_engine(*args: Any, **kwargs: Any) -> Engine: ...
def engine_from_config(configuration: Any, prefix: str = ..., **kwargs: Any) -> Engine: ...
