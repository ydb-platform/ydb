from collections.abc import Callable, Sequence
from typing import Any, TypeVar

__all__ = ["paginate"]

from .bases import AbstractParams
from .config import Config
from .flow import flow_expr, run_sync_flow
from .flows import generic_flow
from .types import AdditionalData, SyncItemsTransformer
from .utils import check_installed_extensions

T = TypeVar("T")


def paginate(
    sequence: Sequence[T],
    params: AbstractParams | None = None,
    length_function: Callable[[Sequence[T]], int] | None = None,
    *,
    safe: bool = False,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    if not safe:
        check_installed_extensions()

    if length_function is None:
        length_function = len

    return run_sync_flow(
        generic_flow(
            limit_offset_flow=flow_expr(lambda r: sequence[r.as_slice()]),
            total_flow=flow_expr(lambda: length_function(sequence)),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )
