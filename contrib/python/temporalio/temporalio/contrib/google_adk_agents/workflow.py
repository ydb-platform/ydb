"""Workflow utilities for Google ADK agents integration with Temporal."""

import functools
import inspect
import types
import typing
from dataclasses import dataclass, field
from typing import Any, Callable, cast

import temporalio.workflow
from temporalio import workflow

_TOOL_CONTEXT_PARAM = "tool_context"


@dataclass(frozen=True)
class ToolContextSnapshot:
    """Serializable snapshot of the ADK ``ToolContext`` for activity-backed tools.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    ADK's ``ToolContext`` holds live, non-serializable objects, so it cannot
    cross the activity boundary: activity inputs are sent to the server and
    may run on a different worker than the workflow. This snapshot carries the
    serializable subset instead.

    Declare a parameter named ``tool_context`` annotated with this type (or
    ``ToolContextSnapshot | None``) on an activity wrapped by
    :func:`activity_as_tool`:

    .. code-block:: python

        @activity.defn
        async def get_weather(query: str, tool_context: ToolContextSnapshot) -> dict:
            db_url = tool_context.state.get("url", "")
            ...

    Exactly like a native ADK function tool's ``tool_context`` parameter, it
    is excluded from the LLM-facing tool schema and filled in at invocation
    time — here with a snapshot taken from the live ``ToolContext`` before the
    activity is scheduled.

    When running under Temporal, the entire session state crosses the
    activity boundary: every value in it must be serializable by the
    configured data converter and the total size must fit within payload
    limits, even for keys the tool never reads. A non-serializable value
    fails the workflow task when the activity is scheduled. Local ADK runs
    pass the snapshot in memory and have no such constraint.

    The snapshot is one-way and should be treated as read-only: mutating it
    inside the activity does not propagate back to the session (and in local
    runs nested values may alias the live session state, so mutating them can
    corrupt the session). To modify session state, return the needed
    information from the activity and apply it in workflow-side code (for
    example an ADK callback or a plain tool function).

    Attributes:
        state: The session state visible to this tool call, as a plain dict.
        function_call_id: The id of the function call being handled, when
            available.
    """

    state: dict[str, Any] = field(default_factory=dict)
    function_call_id: str | None = None


def _annotation_members(annotation: Any) -> tuple[Any, ...]:
    """Returns a union annotation's members, or the annotation itself."""
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:  # pyright: ignore[reportDeprecated]
        return typing.get_args(annotation)
    return (annotation,)


def _annotation_display(members: tuple[Any, ...]) -> str:
    """Renders annotation members for error messages."""
    return " | ".join(
        "None" if member is type(None) else getattr(member, "__name__", str(member))
        for member in members
    )


def _adk_context_error(
    activity_def: Callable, name: str, annotation_name: str
) -> ValueError:
    """Builds the error for a parameter annotated with a live ADK context."""
    return ValueError(
        f"Activity '{activity_def.__name__}' declares '{name}:"
        f" {annotation_name}', but ADK context objects are not serializable"
        " and cannot be activity arguments. Declare a parameter named"
        " 'tool_context' annotated with ToolContextSnapshot instead to receive"
        " the serializable subset (session state and function-call id)."
    )


def _validated_tool_context_parameter(
    activity_def: Callable, parameter: inspect.Parameter, members: tuple[Any, ...]
) -> inspect.Parameter:
    """Returns the ``tool_context`` parameter after validating its annotation.

    The parameter must be annotated with :class:`ToolContextSnapshot` or
    ``ToolContextSnapshot | None``.
    """
    if parameter.annotation is inspect.Parameter.empty:
        raise ValueError(
            f"Activity '{activity_def.__name__}' has an unannotated"
            " 'tool_context' parameter. Annotate it with ToolContextSnapshot"
            " to receive the serializable subset of the ADK tool context"
            " (session state and function-call id)."
        )
    non_none = [member for member in members if member is not type(None)]
    if non_none == [ToolContextSnapshot]:
        return parameter
    annotation_name = _annotation_display(members)
    if any(
        getattr(member, "__module__", "").startswith("google.adk")
        for member in non_none
    ):
        raise _adk_context_error(activity_def, _TOOL_CONTEXT_PARAM, annotation_name)
    raise ValueError(
        f"Activity '{activity_def.__name__}' has a 'tool_context' parameter"
        f" annotated with {annotation_name}. The name 'tool_context' is"
        " reserved by ADK for context injection; annotate the parameter with"
        " ToolContextSnapshot to receive the serializable subset of the tool"
        " context."
    )


def _tool_context_parameter(activity_def: Callable) -> inspect.Parameter | None:
    """Validates context-related parameters and returns the ``tool_context`` one.

    ADK injects the live context into the first parameter annotated with an
    ADK context type (regardless of name), or failing that into one named
    ``tool_context``, and excludes that parameter from the LLM-facing tool
    schema. Live context objects cannot cross the activity boundary, so the
    only supported declaration is a parameter named ``tool_context`` annotated
    with :class:`ToolContextSnapshot` (or ``ToolContextSnapshot | None``);
    anything else ADK would treat as a context parameter is rejected at wrap
    time, as is a misplaced ToolContextSnapshot annotation that would leak
    into the tool schema.
    """
    try:
        hints = typing.get_type_hints(activity_def)
    except Exception:
        hints = {}
    adk_context: type[Any] | None
    try:
        from google.adk.tools.tool_context import ToolContext

        adk_context = ToolContext
    except ImportError:
        adk_context = None
    tool_context_parameter: inspect.Parameter | None = None
    for name, parameter in inspect.signature(activity_def).parameters.items():
        members = _annotation_members(hints.get(name, parameter.annotation))
        if name == _TOOL_CONTEXT_PARAM:
            tool_context_parameter = _validated_tool_context_parameter(
                activity_def, parameter, members
            )
        elif adk_context is not None and any(
            member is adk_context for member in members
        ):
            raise _adk_context_error(activity_def, name, _annotation_display(members))
        elif any(member is ToolContextSnapshot for member in members):
            raise ValueError(
                f"Activity '{activity_def.__name__}' annotates parameter"
                f" '{name}' with ToolContextSnapshot, but ADK only injects the"
                " tool context into a parameter named 'tool_context'; under"
                " any other name it would appear in the LLM-facing tool"
                " schema. Rename the parameter to 'tool_context'."
            )
    return tool_context_parameter


def _snapshot_tool_context(tool_context: Any) -> ToolContextSnapshot:
    """Builds the serializable snapshot from a live ADK ``ToolContext``."""
    state: dict[str, Any] = {}
    state_object = getattr(tool_context, "state", None)
    if state_object is not None:
        to_dict = getattr(state_object, "to_dict", None)
        state = dict(cast(Any, to_dict() if callable(to_dict) else state_object))
    return ToolContextSnapshot(
        state=state,
        function_call_id=getattr(tool_context, "function_call_id", None),
    )


def activity_as_tool(activity_def: Callable, **kwargs: Any) -> Callable:
    """Decorator/Wrapper to wrap a Temporal Activity as an ADK Tool.

    .. warning::
        This function is experimental and may change in future versions.
        Use with caution in production environments.

    This ensures the activity's signature is preserved for ADK's tool schema generation
    while marking it as a tool that executes via 'workflow.execute_activity'.

    If the activity declares a parameter named ``tool_context``, it must be
    annotated with :class:`ToolContextSnapshot` (or ``ToolContextSnapshot |
    None``). ADK excludes the parameter from the tool schema and injects the
    live ``ToolContext`` into the wrapper, which passes the activity a
    serializable snapshot of it (session state and function-call id) in that
    parameter's position. Annotating any parameter with a live ADK context
    type raises ``ValueError`` at wrap time, since ADK would inject the
    non-serializable context into it regardless of its name.
    """
    tool_context_param = _tool_context_parameter(activity_def)

    @functools.wraps(activity_def)
    async def wrapper(*args: Any, **kw: Any):
        # ADK injects the live ToolContext by name; replace it with the
        # serializable snapshot the activity actually declares.
        if tool_context_param is not None and _TOOL_CONTEXT_PARAM in kw:
            kw[_TOOL_CONTEXT_PARAM] = _snapshot_tool_context(kw[_TOOL_CONTEXT_PARAM])

        # Inspect signature to bind arguments
        sig = inspect.signature(activity_def)
        bound = sig.bind(*args, **kw)
        bound.apply_defaults()

        # Convert to positional args for Temporal
        activity_args = list(bound.arguments.values())

        # Decorator kwargs are defaults.
        options = kwargs.copy()

        if not temporalio.workflow.in_workflow():
            # If executed outside a workflow, like when doing local adk runs, use the function directly
            result = activity_def(*args, **kw)
            if inspect.isawaitable(result):
                return await result
            else:
                return result

        if not activity_args:
            return await workflow.execute_activity(activity_def, **options)
        if len(activity_args) == 1:
            return await workflow.execute_activity(
                activity_def, activity_args[0], **options
            )
        return await workflow.execute_activity(
            activity_def, args=activity_args, **options
        )

    # functools.wraps copies name/doc/module/annotations/qualname/dict.
    # Signature must be set explicitly since the wrapper uses *args/**kw.
    setattr(wrapper, "__signature__", inspect.signature(activity_def))

    return wrapper
