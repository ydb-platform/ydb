from typing import Awaitable, Callable, Optional, Sequence

from .amap import amap
from .types import T
from .utils import check_no_lambdas


async def run_any(
    async_fns: Sequence[Callable[[], Awaitable[T]]],
    *,
    max_at_once: Optional[int] = None,
    max_per_second: Optional[float] = None,
) -> T:
    check_no_lambdas(async_fns, entrypoint="aiometer.run_any")

    async with amap(
        lambda fn: fn(),
        async_fns,
        max_at_once=max_at_once,
        max_per_second=max_per_second,
    ) as results:
        return await (results.__aiter__()).__anext__()
