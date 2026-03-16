"""Workflow-specific primitives for working with the OpenAI Agents SDK in a workflow context"""

import functools
import inspect
import json
from datetime import timedelta
from typing import Any, Callable, Optional, Type, Union, overload

import nexusrpc
from agents import (
    Agent,
    RunContextWrapper,
    Tool,
)
from agents.function_schema import DocstringStyle, function_schema
from agents.tool import (
    FunctionTool,
    ToolErrorFunction,
    ToolFunction,
    ToolParams,
    default_tool_error_function,
    function_tool,
)
from agents.util._types import MaybeAwaitable

from temporalio import activity
from temporalio import workflow as temporal_workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.exceptions import ApplicationError, TemporalError
from temporalio.workflow import ActivityCancellationType, VersioningIntent


def activity_as_tool(
    fn: Callable,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: Priority = Priority.default,
) -> Tool:
    """Convert a single Temporal activity function to an OpenAI agent tool.

    .. warning::
        This API is experimental and may change in future versions.
        Use with caution in production environments.

    This function takes a Temporal activity function and converts it into an
    OpenAI agent tool that can be used by the agent to execute the activity
    during workflow execution. The tool will automatically handle the conversion
    of inputs and outputs between the agent and the activity. Note that if you take a context,
    mutation will not be persisted, as the activity may not be running in the same location.

    Args:
        fn: A Temporal activity function to convert to a tool.
        For other arguments, refer to :py:mod:`workflow` :py:meth:`start_activity`

    Returns:
        An OpenAI agent tool that wraps the provided activity.

    Raises:
        ApplicationError: If the function is not properly decorated as a Temporal activity.

    Example:
        >>> @activity.defn
        >>> def process_data(input: str) -> str:
        ...     return f"Processed: {input}"
        >>>
        >>> # Create tool with custom activity options
        >>> tool = activity_as_tool(
        ...     process_data,
        ...     start_to_close_timeout=timedelta(seconds=30),
        ...     retry_policy=RetryPolicy(maximum_attempts=3),
        ...     heartbeat_timeout=timedelta(seconds=10)
        ... )
        >>> # Use tool with an OpenAI agent
    """
    ret = activity._Definition.from_callable(fn)
    if not ret:
        raise ApplicationError(
            "Bare function without tool and activity decorators is not supported",
            "invalid_tool",
        )
    if ret.name is None:
        raise ApplicationError(
            "Input activity must have a name to be made into a tool",
            "invalid_tool",
        )
    # If the provided callable has a first argument of `self`, partially apply it with the same metadata
    # The actual instance will be picked up by the activity execution, the partially applied function will never actually be executed
    params = list(inspect.signature(fn).parameters.keys())
    if len(params) > 0 and params[0] == "self":
        partial = functools.partial(fn, None)
        setattr(partial, "__name__", fn.__name__)
        partial.__annotations__ = getattr(fn, "__annotations__")
        setattr(
            partial,
            "__temporal_activity_definition",
            getattr(fn, "__temporal_activity_definition"),
        )
        partial.__doc__ = fn.__doc__
        fn = partial
    schema = function_schema(fn)

    async def run_activity(ctx: RunContextWrapper[Any], input: str) -> Any:
        try:
            json_data = json.loads(input)
        except Exception as e:
            raise ApplicationError(
                f"Invalid JSON input for tool {schema.name}: {input}"
            ) from e

        # Activities don't support keyword only arguments, so we can ignore the kwargs_dict return
        args, _ = schema.to_call_args(schema.params_pydantic_model(**json_data))

        # Add the context to the arguments if it takes that
        if schema.takes_context:
            args = [ctx] + args
        result = await temporal_workflow.execute_activity(
            ret.name,  # type: ignore
            args=args,
            task_queue=task_queue,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=retry_policy,
            cancellation_type=cancellation_type,
            activity_id=activity_id,
            versioning_intent=versioning_intent,
            summary=summary or schema.description,
            priority=priority,
        )
        try:
            return str(result)
        except Exception as e:
            raise ToolSerializationError(
                "You must return a string representation of the tool output, or something we can call str() on"
            ) from e

    return FunctionTool(
        name=schema.name,
        description=schema.description or "",
        params_json_schema=schema.params_json_schema,
        on_invoke_tool=run_activity,
        strict_json_schema=True,
    )


def nexus_operation_as_tool(
    operation: nexusrpc.Operation[Any, Any],
    *,
    service: Type[Any],
    endpoint: str,
    schedule_to_close_timeout: Optional[timedelta] = None,
) -> Tool:
    """Convert a Nexus operation into an OpenAI agent tool.

    .. warning::
        This API is experimental and may change in future versions.
        Use with caution in production environments.

    This function takes a Nexus operation and converts it into an
    OpenAI agent tool that can be used by the agent to execute the operation
    during workflow execution. The tool will automatically handle the conversion
    of inputs and outputs between the agent and the operation.

    Args:
        fn: A Nexus operation to convert into a tool.
        service: The Nexus service class that contains the operation.
        endpoint: The Nexus endpoint to use for the operation.

    Returns:
        An OpenAI agent tool that wraps the provided operation.

    Example:
        >>> @nexusrpc.service
        ... class WeatherService:
        ...     get_weather_object_nexus_operation: nexusrpc.Operation[WeatherInput, Weather]
        >>>
        >>> # Create tool with custom activity options
        >>> tool = nexus_operation_as_tool(
        ...     WeatherService.get_weather_object_nexus_operation,
        ...     service=WeatherService,
        ...     endpoint="weather-service",
        ... )
        >>> # Use tool with an OpenAI agent
    """

    def operation_callable(input):
        raise NotImplementedError("This function definition is used as a type only")

    operation_callable.__annotations__ = {
        "input": operation.input_type,
        "return": operation.output_type,
    }
    operation_callable.__name__ = operation.name

    schema = function_schema(operation_callable)

    async def run_operation(ctx: RunContextWrapper[Any], input: str) -> Any:
        try:
            json_data = json.loads(input)
        except Exception as e:
            raise ApplicationError(
                f"Invalid JSON input for tool {schema.name}: {input}"
            ) from e

        nexus_client = temporal_workflow.create_nexus_client(
            service=service, endpoint=endpoint
        )
        args, _ = schema.to_call_args(schema.params_pydantic_model(**json_data))
        assert len(args) == 1, "Nexus operations must have exactly one argument"
        [arg] = args
        result = await nexus_client.execute_operation(
            operation,
            arg,
            schedule_to_close_timeout=schedule_to_close_timeout,
        )
        try:
            return str(result)
        except Exception as e:
            raise ToolSerializationError(
                "You must return a string representation of the tool output, or something we can call str() on"
            ) from e

    return FunctionTool(
        name=schema.name,
        description=schema.description or "",
        params_json_schema=schema.params_json_schema,
        on_invoke_tool=run_operation,
        strict_json_schema=True,
    )


class ToolSerializationError(TemporalError):
    """Error that occurs when a tool output could not be serialized.

    .. warning::
        This exception is experimental and may change in future versions.
        Use with caution in production environments.

    This exception is raised when a tool (created from an activity or Nexus operation)
    returns a value that cannot be properly serialized for use by the OpenAI agent.
    All tool outputs must be convertible to strings for the agent to process them.

    The error typically occurs when:
    - A tool returns a complex object that doesn't have a meaningful string representation
    - The returned object cannot be converted using str()
    - Custom serialization is needed but not implemented

    Example:
        >>> @activity.defn
        >>> def problematic_tool() -> ComplexObject:
        ...     return ComplexObject()  # This might cause ToolSerializationError

    To fix this error, ensure your tool returns string-convertible values or
    modify the tool to return a string representation of the result.
    """
