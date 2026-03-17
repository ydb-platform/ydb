from typing import Awaitable, Callable, List, NamedTuple, Optional, Sequence, cast

import anyio
from anyio.streams.memory import MemoryObjectSendStream

from .meters import HardLimitMeter, Meter, MeterState, RateLimitMeter
from .types import T


class _Config(NamedTuple):
    include_index: bool
    send_to: Optional[MemoryObjectSendStream]
    meter_states: List[MeterState]


async def _worker(
    async_fn: Callable[[T], Awaitable], index: int, value: T, config: _Config
) -> None:
    result = await async_fn(value)

    if config.send_to is not None:
        if config.include_index:
            result = (index, result)
        await config.send_to.send(result)

    for state in config.meter_states:
        await state.notify_task_finished()


async def run_on_each(
    async_fn: Callable[[T], Awaitable],
    args: Sequence[T],
    *,
    max_at_once: Optional[int] = None,
    max_per_second: Optional[float] = None,
    _include_index: bool = False,
    _send_to: Optional[MemoryObjectSendStream] = None,
) -> None:
    meters: List[Meter] = []

    if max_at_once is not None:
        meters.append(HardLimitMeter(max_at_once))
    if max_per_second is not None:
        meters.append(RateLimitMeter(max_per_second))

    meter_states = [await meter.new_state() for meter in meters]

    config = _Config(
        include_index=_include_index, send_to=_send_to, meter_states=meter_states
    )

    async with anyio.create_task_group() as task_group:
        for index, value in enumerate(args):
            for state in meter_states:
                await state.wait_task_can_start()

            for state in meter_states:
                await state.notify_task_started()

            task_group.start_soon(
                cast(Callable, _worker), async_fn, index, value, config
            )
