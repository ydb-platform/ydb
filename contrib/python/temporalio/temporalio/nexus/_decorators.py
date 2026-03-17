from __future__ import annotations

from typing import (
    Awaitable,
    Callable,
    Optional,
    TypeVar,
    Union,
    overload,
)

import nexusrpc
from nexusrpc import InputT, OutputT
from nexusrpc.handler import (
    OperationHandler,
    StartOperationContext,
)

from ._operation_context import WorkflowRunOperationContext
from ._operation_handlers import WorkflowRunOperationHandler
from ._token import WorkflowHandle
from ._util import (
    get_callable_name,
    get_workflow_run_start_method_input_and_output_type_annotations,
    set_operation_factory,
)

ServiceHandlerT = TypeVar("ServiceHandlerT")


@overload
def workflow_run_operation(
    start: Callable[
        [ServiceHandlerT, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ],
) -> Callable[
    [ServiceHandlerT, WorkflowRunOperationContext, InputT],
    Awaitable[WorkflowHandle[OutputT]],
]: ...


@overload
def workflow_run_operation(
    *,
    name: Optional[str] = None,
) -> Callable[
    [
        Callable[
            [ServiceHandlerT, WorkflowRunOperationContext, InputT],
            Awaitable[WorkflowHandle[OutputT]],
        ]
    ],
    Callable[
        [ServiceHandlerT, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ],
]: ...


def workflow_run_operation(
    start: Optional[
        Callable[
            [ServiceHandlerT, WorkflowRunOperationContext, InputT],
            Awaitable[WorkflowHandle[OutputT]],
        ]
    ] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Callable[
        [ServiceHandlerT, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ],
    Callable[
        [
            Callable[
                [ServiceHandlerT, WorkflowRunOperationContext, InputT],
                Awaitable[WorkflowHandle[OutputT]],
            ]
        ],
        Callable[
            [ServiceHandlerT, WorkflowRunOperationContext, InputT],
            Awaitable[WorkflowHandle[OutputT]],
        ],
    ],
]:
    """Decorator marking a method as the start method for a workflow-backed operation."""

    def decorator(
        start: Callable[
            [ServiceHandlerT, WorkflowRunOperationContext, InputT],
            Awaitable[WorkflowHandle[OutputT]],
        ],
    ) -> Callable[
        [ServiceHandlerT, WorkflowRunOperationContext, InputT],
        Awaitable[WorkflowHandle[OutputT]],
    ]:
        (
            input_type,
            output_type,
        ) = get_workflow_run_start_method_input_and_output_type_annotations(start)

        def operation_handler_factory(
            self: ServiceHandlerT,
        ) -> OperationHandler[InputT, OutputT]:
            async def _start(
                ctx: StartOperationContext, input: InputT
            ) -> WorkflowHandle[OutputT]:
                return await start(
                    self,
                    WorkflowRunOperationContext._from_start_operation_context(ctx),
                    input,
                )

            _start.__doc__ = start.__doc__
            return WorkflowRunOperationHandler(_start)

        method_name = get_callable_name(start)
        nexusrpc.set_operation_definition(
            operation_handler_factory,
            nexusrpc.Operation(
                name=name or method_name,
                method_name=method_name,
                input_type=input_type,
                output_type=output_type,
            ),
        )

        set_operation_factory(start, operation_handler_factory)
        return start

    if start is None:
        return decorator

    return decorator(start)
