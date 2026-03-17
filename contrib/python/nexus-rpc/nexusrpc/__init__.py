"""
nexusrpc is a library for building Nexus handlers.

See https://github.com/nexus-rpc and https://github.com/nexus-rpc/api/blob/main/SPEC.md.

Nexus is a synchronous RPC protocol. Arbitrary duration operations are modeled on top of
a set of pre-defined synchronous RPCs.

A Nexus caller calls a handler. The handler may respond inline (synchronous response) or
return a token referencing the ongoing operation (asynchronous response). The caller can
cancel an asynchronous operation, check for its outcome, or fetch its current state. The
caller can also specify a callback URL, which the handler uses to deliver the result of
an asynchronous operation when it is ready.
"""

from __future__ import annotations

from . import handler
from ._common import (
    HandlerError,
    HandlerErrorType,
    InputT,
    Link,
    OperationError,
    OperationErrorState,
    OperationInfo,
    OperationState,
    OutputT,
)
from ._serializer import Content, LazyValue
from ._service import Operation, ServiceDefinition, service
from ._util import (
    get_operation_definition,
    get_service_definition,
    set_operation_definition,
)

__all__ = [
    "Content",
    "get_operation_definition",
    "get_service_definition",
    "handler",
    "HandlerError",
    "HandlerErrorType",
    "InputT",
    "LazyValue",
    "Link",
    "Operation",
    "OperationError",
    "OperationErrorState",
    "OperationInfo",
    "OperationState",
    "OutputT",
    "service",
    "ServiceDefinition",
    "set_operation_definition",
]
