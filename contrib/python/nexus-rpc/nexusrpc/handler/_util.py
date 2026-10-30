from __future__ import annotations

import typing
import warnings
from collections.abc import Awaitable
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from nexusrpc.handler import StartOperationContext

if TYPE_CHECKING:
    from nexusrpc import InputT, OutputT


ServiceHandlerT = TypeVar("ServiceHandlerT")


def get_start_method_input_and_output_type_annotations(
    start: Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
) -> tuple[
    Optional[type[InputT]],
    Optional[type[OutputT]],
]:
    """Extract input and output type annotations from a start method.

    Args:
        start: A start method with signature (self, ctx: StartOperationContext, input: I) -> O

    Returns:
        A tuple of (input_type, output_type) where:

        - ``None`` means the type annotation is missing or could not be extracted.
          This is valid when a service definition provides the types.
        - ``type(None)`` (NoneType) means the annotation explicitly specified ``None``
          as the type (e.g., ``input: None`` or ``-> None``).

        When ``None`` is returned for either type, the caller should handle it based on
        whether a service definition is available:

        - If a service definition is provided, the types from the service definition
          will be used and type validation against the handler is skipped.
        - If no service definition is provided, an error will be raised downstream
          when attempting to create an ``OperationDefinition``.
    """
    try:
        type_annotations = typing.get_type_hints(start)
    except TypeError:
        return None, None
    output_type = type_annotations.pop("return", None)

    if len(type_annotations) != 2:
        input_type = None
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, StartOperationContext):
            warnings.warn(
                f"Expected first parameter of {start} to be an instance of "
                f"StartOperationContext, but is {ctx_type}."
            )
            input_type = None

    return input_type, output_type
