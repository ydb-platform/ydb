from __future__ import annotations

import functools
import inspect
import typing
import warnings
from collections.abc import Awaitable, Callable
from typing import (
    Any,
)

import nexusrpc
from nexusrpc import (
    InputT,
    OutputT,
)

from temporalio.nexus._operation_context import (
    TemporalStartOperationContext,
    WorkflowRunOperationContext,
)
from temporalio.nexus._temporal_client import (
    TemporalNexusClient,
    TemporalOperationResult,
)
from temporalio.types import NexusServiceType

from ._token import (
    WorkflowHandle as WorkflowHandle,
)


def get_workflow_run_start_method_input_and_output_type_annotations(
    start: Callable[
        [NexusServiceType, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ],
) -> tuple[
    type[InputT] | None,
    type[OutputT] | None,
]:
    """Return operation input and output types.

    ``start`` must be a type-annotated start method that returns a
    :py:class:`temporalio.nexus.WorkflowHandle`.
    """
    return _get_wrapped_start_method_input_and_output_type_annotations(
        start,
        expected_param_types=(WorkflowRunOperationContext,),
        expected_return_origin=WorkflowHandle,
    )


def get_temporal_operation_start_method_input_and_output_type_annotations(
    start: Callable[
        [
            NexusServiceType,
            TemporalStartOperationContext,
            TemporalNexusClient,
            InputT,
        ],
        Awaitable[TemporalOperationResult[OutputT]],
    ],
) -> tuple[
    type[InputT] | None,
    type[OutputT] | None,
]:
    """Return operation input and output types.

    ``start`` must be a type-annotated start method that returns a
    :py:class:`temporalio.nexus.TemporalOperationResult`.
    """
    return _get_wrapped_start_method_input_and_output_type_annotations(
        start,
        expected_param_types=(
            TemporalStartOperationContext,
            TemporalNexusClient,
        ),
        expected_return_origin=TemporalOperationResult,
    )


def _get_wrapped_start_method_input_and_output_type_annotations(
    start: Callable[..., Any],
    *,
    expected_param_types: tuple[type[Any], ...],
    expected_return_origin: type[Any],
) -> tuple[
    type[Any] | None,
    type[Any] | None,
]:
    input_type, output_type = _get_start_method_input_and_output_type_annotations(
        start,
        expected_param_types=expected_param_types,
    )
    origin_type = typing.get_origin(output_type)
    if not origin_type:
        output_type = None
    elif not issubclass(origin_type, expected_return_origin):
        warnings.warn(
            f"Expected return type of {start.__name__} to be a subclass of "
            f"{expected_return_origin.__name__}, "
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
    start: Callable[..., Any],
    *,
    expected_param_types: tuple[type[Any], ...],
) -> tuple[
    type[Any] | None,
    type[Any] | None,
]:
    try:
        type_annotations = typing.get_type_hints(start)
    except TypeError:
        warnings.warn(
            f"Expected decorated start method {start} to have type annotations"
        )
        return None, None
    output_type = type_annotations.pop("return", None)
    expected_parameter_count = len(expected_param_types) + 1

    if len(type_annotations) != expected_parameter_count:
        suffix = f": {type_annotations}" if type_annotations else ""
        warnings.warn(
            f"Expected decorated start method {start} to have exactly "
            f"{expected_parameter_count} type-annotated parameters, "
            f"but it has {len(type_annotations)}"
            f"{suffix}."
        )
        input_type = None
    else:
        *param_types, input_type = type_annotations.values()
        for index, (param_type, expected_param_type) in enumerate(
            zip(param_types, expected_param_types), start=1
        ):
            if not issubclass(expected_param_type, param_type):
                warnings.warn(
                    f"Expected parameter {index} of {start} to be an instance of "
                    f"{expected_param_type.__name__}, but is {param_type}."
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
    Callable[[Any], Any] | None,
    nexusrpc.Operation[Any, Any] | None,
]:
    """Return the :py:class:`nexusrpc.Operation` for the object along with the factory function.

    ``obj`` should be a decorated operation start method.
    """
    op_defn = nexusrpc.get_operation(obj)
    if op_defn:
        factory = obj
    else:
        if factory := getattr(obj, "__nexus_operation_factory__", None):
            op_defn = nexusrpc.get_operation(factory)
    if not isinstance(op_defn, nexusrpc.Operation):
        return None, None
    return factory, op_defn


# TODO(nexus-preview) Copied from nexusrpc
def set_operation_factory(
    obj: Any,
    operation_factory: Callable[[Any], Any],
) -> None:
    """Set the :py:class:`nexusrpc.handler.OperationHandler` factory for this object.

    ``obj`` should be an operation start method.
    """
    setattr(obj, "__nexus_operation_factory__", operation_factory)


# Copied from https://github.com/modelcontextprotocol/python-sdk
#
# Copyright (c) 2024 Anthropic, PBC.
#
# This file is licensed under the MIT License.
def is_async_callable(obj: Any) -> bool:
    """Return True if ``obj`` is an async callable.

    Supports partials of async callable class instances.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(getattr(obj, "__call__", None))
    )
