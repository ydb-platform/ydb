from collections.abc import Iterable
from itertools import islice
from typing import Any

__all__ = [
    "LimitOffsetPage",
    "LimitOffsetParams",
    "Page",
    "Params",
    "paginate",
]

from .bases import AbstractParams
from .config import Config
from .flow import flow_expr, run_sync_flow
from .flows import generic_flow
from .optional import OptionalLimitOffsetPage as LimitOffsetPage
from .optional import OptionalLimitOffsetParams as LimitOffsetParams
from .optional import OptionalPage as Page
from .optional import OptionalParams as Params
from .types import AdditionalData, SyncItemsTransformer


def paginate(
    iterable: Iterable[Any],
    params: AbstractParams | None = None,
    total: int | None = None,
    *,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    return run_sync_flow(
        generic_flow(
            limit_offset_flow=flow_expr(
                lambda r: [
                    *islice(
                        iterable,
                        r.as_slice().start,
                        r.as_slice().stop,
                    )
                ],
            ),
            total_flow=flow_expr(lambda: total),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )
