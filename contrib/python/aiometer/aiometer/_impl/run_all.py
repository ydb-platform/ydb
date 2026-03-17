from typing import Awaitable, Callable, Dict, List, Optional, Sequence

from .amap import amap
from .types import T
from .utils import check_no_lambdas, list_from_indexed_dict


async def run_all(
    async_fns: Sequence[Callable[[], Awaitable[T]]],
    *,
    max_at_once: Optional[int] = None,
    max_per_second: Optional[float] = None,
) -> List[T]:
    check_no_lambdas(async_fns, entrypoint="aiometer.run_all")

    results: Dict[int, T] = {}

    async with amap(
        lambda fn: fn(),
        async_fns,
        max_at_once=max_at_once,
        max_per_second=max_per_second,
        _include_index=True,
    ) as amap_results:
        async for index, result in amap_results:
            results[index] = result

    return list_from_indexed_dict(results)
