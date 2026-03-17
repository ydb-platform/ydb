import asyncio
from functools import reduce
from pathlib import Path

import pytest

import aioftp


@pytest.mark.asyncio
async def test_patched_sleep(skip_sleep):
    await asyncio.sleep(10)
    assert skip_sleep.is_close(10)


SIZE = 3 * 100 * 1024  # 300KiB


@pytest.mark.parametrize("times", [10, 20, 30])
@pytest.mark.parametrize("type", ["read", "write"])
@pytest.mark.parametrize("direction", ["download", "upload"])
@pytest.mark.asyncio
async def test_client_side_throttle(
    pair_factory,
    skip_sleep,
    times,
    type,
    direction,
):
    async with pair_factory() as pair:
        await pair.make_server_files("foo", size=SIZE)
        await pair.make_client_files("foo", size=SIZE)
        getattr(pair.client.throttle, type).limit = SIZE / times
        await getattr(pair.client, direction)("foo")
        if (type, direction) in {("read", "download"), ("write", "upload")}:
            assert skip_sleep.is_close(times)
        else:
            assert skip_sleep.is_close(0)


@pytest.mark.parametrize("times", [10, 20, 30])
@pytest.mark.parametrize("users", [1, 2, 3])
@pytest.mark.parametrize("throttle_direction", ["read", "write"])
@pytest.mark.parametrize("data_direction", ["download", "upload"])
@pytest.mark.parametrize(
    "throttle_level",
    [
        "throttle",
        "throttle_per_connection",
    ],
)
@pytest.mark.asyncio
async def test_server_side_throttle(
    pair_factory,
    skip_sleep,
    times,
    users,
    throttle_direction,
    data_direction,
    throttle_level,
):
    async with pair_factory() as pair:
        names = []
        for i in range(users):
            name = f"foo{i}"
            names.append(name)
            await pair.make_server_files(name, size=SIZE)
        throttle = reduce(
            getattr,
            [throttle_level, throttle_direction],
            pair.server,
        )
        throttle.limit = SIZE / times
        clients = []
        for name in names:
            c = aioftp.Client(path_io_factory=aioftp.MemoryPathIO)
            async with c.path_io.open(Path(name), "wb") as f:
                await f.write(b"-" * SIZE)
            await c.connect(pair.server.server_host, pair.server.server_port)
            await c.login()
            clients.append(c)
        coros = [getattr(c, data_direction)(n) for c, n in zip(clients, names)]
        await asyncio.gather(*coros)
        await asyncio.gather(*[c.quit() for c in clients])
    throttled = {("read", "upload"), ("write", "download")}
    if (throttle_direction, data_direction) not in throttled:
        assert skip_sleep.is_close(0)
    else:
        t = times
        if throttle_level == "throttle":  # global
            t *= users
        assert skip_sleep.is_close(t)
