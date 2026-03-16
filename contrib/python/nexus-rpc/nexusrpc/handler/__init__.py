"""
Components for implementing Nexus handlers.

Server/worker authors will use this module to create the top-level Nexus handlers
responsible for dispatching requests to Nexus operations.

Nexus service/operation authors will use this module to implement operation handler
methods within service handler classes.
"""

from __future__ import annotations

from ._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    OperationContext,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from ._core import Handler as Handler
from ._decorators import service_handler, sync_operation
from ._operation_handler import OperationHandler as OperationHandler

__all__ = [
    "CancelOperationContext",
    "FetchOperationInfoContext",
    "FetchOperationResultContext",
    "Handler",
    "OperationContext",
    "OperationHandler",
    "service_handler",
    "StartOperationContext",
    "StartOperationResultAsync",
    "StartOperationResultSync",
    "sync_operation",
]
