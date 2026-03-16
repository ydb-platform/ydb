from __future__ import annotations

import typing
import warnings
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Optional,
    Type,
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
    Optional[Type[InputT]],
    Optional[Type[OutputT]],
]:
    """Return operation input and output types.

    `start` must be a type-annotated start method that returns a synchronous result.
    """
    try:
        type_annotations = typing.get_type_hints(start)
    except TypeError:
        # TODO(preview): stacklevel
        warnings.warn(
            f"Expected decorated start method {start} to have type annotations"
        )
        return None, None
    output_type = type_annotations.pop("return", None)

    if len(type_annotations) != 2:
        # TODO(preview): stacklevel
        suffix = f": {type_annotations}" if type_annotations else ""
        warnings.warn(
            f"Expected decorated start method {start} to have exactly 2 "
            f"type-annotated parameters (ctx and input), but it has {len(type_annotations)}"
            f"{suffix}."
        )
        input_type = None
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, StartOperationContext):
            # TODO(preview): stacklevel
            warnings.warn(
                f"Expected first parameter of {start} to be an instance of "
                f"StartOperationContext, but is {ctx_type}."
            )
            input_type = None

    return input_type, output_type
