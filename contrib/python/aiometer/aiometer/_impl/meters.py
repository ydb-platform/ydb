import anyio

from .utils import check_strictly_positive


class MeterState:
    async def wait_task_can_start(self) -> None:
        raise NotImplementedError  # pragma: no cover

    async def notify_task_started(self) -> None:
        raise NotImplementedError  # pragma: no cover

    async def notify_task_finished(self) -> None:
        raise NotImplementedError  # pragma: no cover


class Meter:
    async def new_state(self) -> MeterState:
        raise NotImplementedError  # pragma: no cover


class HardLimitMeter(Meter):
    class State(MeterState):
        def __init__(self, max_at_once: int) -> None:
            self.semaphore = anyio.Semaphore(max_at_once)

        async def wait_task_can_start(self) -> None:
            # anyio semaphore interface has no '.acquire()'.
            await self.semaphore.__aenter__()

        async def notify_task_started(self) -> None:
            pass

        async def notify_task_finished(self) -> None:
            # anyio semaphore interface has no '.release()'.
            await self.semaphore.__aexit__(None, None, None)

    def __init__(self, max_at_once: int) -> None:
        check_strictly_positive("max_at_once", max_at_once)
        self.max_at_once = max_at_once

    async def new_state(self) -> MeterState:
        return type(self).State(self.max_at_once)


class RateLimitMeter(Meter):
    class State(MeterState):
        def __init__(self, max_per_second: float) -> None:
            self.period = 1 / max_per_second
            self.max_per_period = 1  # TODO: make configurable.
            self.next_start_time = 0.0

        @property
        def task_delta(self) -> float:
            return self.period / self.max_per_period

        async def wait_task_can_start(self) -> None:
            while True:
                # NOTE: this is an implementation of the "virtual scheduling" variant
                # of the GCRA algorithm.
                # `next_start_time` represents the TAT (theoretical time of arrival).
                # See: https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm
                now = anyio.current_time()
                next_start_time = max(self.next_start_time, now)
                time_until_start = next_start_time - now
                threshold = self.period - self.task_delta
                if time_until_start <= threshold:
                    break
                await anyio.sleep(max(0, time_until_start - threshold))

        async def notify_task_started(self) -> None:
            now = anyio.current_time()
            self.next_start_time = max(self.next_start_time, now) + self.task_delta

        async def notify_task_finished(self) -> None:
            pass

    def __init__(self, max_per_second: float) -> None:
        check_strictly_positive("max_per_second", max_per_second)
        self.max_per_second = max_per_second

    async def new_state(self) -> MeterState:
        return type(self).State(self.max_per_second)
