from __future__ import annotations

import functools
import inspect
import typing
import warnings
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Type,
    TypeVar,
)

import nexusrpc
from nexusrpc import (
    InputT,
    OutputT,
)

from temporalio.nexus._operation_context import WorkflowRunOperationContext

from ._token import (
    WorkflowHandle as WorkflowHandle,
)

ServiceHandlerT = TypeVar("ServiceHandlerT")


def get_workflow_run_start_method_input_and_output_type_annotations(
    start: Callable[
        [ServiceHandlerT, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ],
) -> tuple[
    Optional[Type[InputT]],
    Optional[Type[OutputT]],
]:
    """Return operation input and output types.

    `start` must be a type-annotated start method that returns a
    :py:class:`temporalio.nexus.WorkflowHandle`.
    """
    input_type, output_type = _get_start_method_input_and_output_type_annotations(start)
    origin_type = typing.get_origin(output_type)
    if not origin_type:
        output_type = None
    elif not issubclass(origin_type, WorkflowHandle):
        warnings.warn(
            f"Expected return type of {start.__name__} to be a subclass of WorkflowHandle, "
            f"but is {output_type}"
        )
        output_type = None

    if output_type:
        args = typing.get_args(output_type)
        if len(args) != 1:
            suffix = f": {args}" if args else ""
            warnings.warn(
                f"Expected return type {output_type} of {start.__name__} to have exactly one type parameter, "
                f"but has {len(args)}{suffix}."
            )
            output_type = None
        else:
            [output_type] = args
    return input_type, output_type


def _get_start_method_input_and_output_type_annotations(
    start: Callable[
        [ServiceHandlerT, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ],
) -> tuple[
    Optional[Type[InputT]],
    Optional[Type[OutputT]],
]:
    try:
        type_annotations = typing.get_type_hints(start)
    except TypeError:
        warnings.warn(
            f"Expected decorated start method {start} to have type annotations"
        )
        return None, None
    output_type = type_annotations.pop("return", None)

    if len(type_annotations) != 2:
        suffix = f": {type_annotations}" if type_annotations else ""
        warnings.warn(
            f"Expected decorated start method {start} to have exactly 2 "
            f"type-annotated parameters (ctx and input), but it has {len(type_annotations)}"
            f"{suffix}."
        )
        input_type = None
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, WorkflowRunOperationContext):
            warnings.warn(
                f"Expected first parameter of {start} to be an instance of "
                f"WorkflowRunOperationContext, but is {ctx_type}."
            )
            input_type = None

    return input_type, output_type


def get_callable_name(fn: Callable[..., Any]) -> str:
    """Return the name of a callable object."""
    method_name = getattr(fn, "__name__", None)
    if not method_name and callable(fn) and hasattr(fn, "__call__"):
        method_name = fn.__class__.__name__
    if not method_name:
        raise TypeError(
            f"Could not determine callable name: "
            f"expected {fn} to be a function or callable instance."
        )
    return method_name


# TODO(nexus-preview) Copied from nexusrpc
def get_operation_factory(
    obj: Any,
) -> tuple[
    Optional[Callable[[Any], Any]],
    Optional[nexusrpc.Operation[Any, Any]],
]:
    """Return the :py:class:`Operation` for the object along with the factory function.

    ``obj`` should be a decorated operation start method.
    """
    op_defn = nexusrpc.get_operation_definition(obj)
    if op_defn:
        factory = obj
    else:
        if factory := getattr(obj, "__nexus_operation_factory__", None):
            op_defn = nexusrpc.get_operation_definition(factory)
    if not isinstance(op_defn, nexusrpc.Operation):
        return None, None
    return factory, op_defn


# TODO(nexus-preview) Copied from nexusrpc
def set_operation_factory(
    obj: Any,
    operation_factory: Callable[[Any], Any],
) -> None:
    """Set the :py:class:`OperationHandler` factory for this object.

    ``obj`` should be an operation start method.
    """
    setattr(obj, "__nexus_operation_factory__", operation_factory)


# Copied from https://github.com/modelcontextprotocol/python-sdk
#
# Copyright (c) 2024 Anthropic, PBC.
#
# This file is licensed under the MIT License.
def is_async_callable(obj: Any) -> bool:
    """Return True if `obj` is an async callable.

    Supports partials of async callable class instances.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(getattr(obj, "__call__", None))
    )
