import functools

import pytest

import aioftp


@pytest.mark.asyncio
async def test_multiply_connections_no_limits(pair_factory):
    Client = functools.partial(
        aioftp.Client,
        path_io_factory=aioftp.MemoryPathIO,
    )
    async with pair_factory() as pair:
        s = pair.server
        clients = [Client() for _ in range(4)]
        for c in clients:
            await c.connect(s.server_host, s.server_port)
            await c.login()
        for c in clients:
            await c.quit()


@pytest.mark.asyncio
async def test_multiply_connections_limited_error(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    Client = functools.partial(
        aioftp.Client,
        path_io_factory=aioftp.MemoryPathIO,
    )
    s = Server(maximum_connections=4)
    async with pair_factory(None, s) as pair:
        s = pair.server
        clients = [Client() for _ in range(4)]
        for c in clients[:-1]:
            await c.connect(s.server_host, s.server_port)
            await c.login()
        with expect_codes_in_exception("421"):
            await clients[-1].connect(s.server_host, s.server_port)
        for c in clients[:-1]:
            await c.quit()


@pytest.mark.asyncio
async def test_multiply_user_commands(pair_factory, Server):
    s = Server(maximum_connections=1)
    async with pair_factory(None, s) as pair:
        for _ in range(10):
            await pair.client.login()


@pytest.mark.asyncio
async def test_multiply_connections_with_user_limited_error(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    Client = functools.partial(
        aioftp.Client,
        path_io_factory=aioftp.MemoryPathIO,
    )
    s = Server([aioftp.User("foo", maximum_connections=4)])
    async with pair_factory(None, s, connected=False) as pair:
        s = pair.server
        clients = [Client() for _ in range(5)]
        for c in clients[:-1]:
            await c.connect(s.server_host, s.server_port)
            await c.login("foo")
        await clients[-1].connect(s.server_host, s.server_port)
        with expect_codes_in_exception("530"):
            await clients[-1].login("foo")
        for c in clients[:-1]:
            await c.quit()


@pytest.mark.asyncio
async def test_multiply_connections_relogin_balanced(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    Client = functools.partial(
        aioftp.Client,
        path_io_factory=aioftp.MemoryPathIO,
    )
    s = Server(maximum_connections=4)
    async with pair_factory(None, s, connected=False) as pair:
        s = pair.server
        clients = [Client() for _ in range(5)]
        for c in clients[:-1]:
            await c.connect(s.server_host, s.server_port)
            await c.login()
        await clients[0].quit()
        await clients[-1].connect(s.server_host, s.server_port)
        await clients[-1].login()
        for c in clients[1:]:
            await c.quit()
