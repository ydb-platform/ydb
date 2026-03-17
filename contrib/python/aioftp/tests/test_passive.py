import pytest


async def not_implemented(connection, rest):
    connection.response("502", ":P")
    return True


@pytest.mark.asyncio
async def test_client_fallback_to_pasv_at_list(pair_factory):
    async with pair_factory(host="127.0.0.1") as pair:
        pair.server.epsv = not_implemented
        await pair.client.list()


@pytest.mark.asyncio
async def test_client_fail_fallback_to_pasv_at_list(
    pair_factory,
    expect_codes_in_exception,
):
    async with pair_factory(host="127.0.0.1") as pair:
        pair.server.commands_mapping["epsv"] = not_implemented
        with expect_codes_in_exception("502"):
            await pair.client.get_passive_connection(commands=["epsv"])
        with expect_codes_in_exception("502"):
            pair.client._passive_commands = ["epsv"]
            await pair.client.get_passive_connection()


@pytest.mark.asyncio
async def test_client_only_passive_list(pair_factory):
    async with pair_factory(host="127.0.0.1") as pair:
        pair.client._passive_commands = ["pasv"]
        await pair.client.list()


@pytest.mark.asyncio
async def test_client_only_enhanced_passive_list(pair_factory):
    async with pair_factory(host="127.0.0.1") as pair:
        pair.client._passive_commands = ["epsv"]
        await pair.client.list()


@pytest.mark.asyncio
async def test_passive_no_choices(pair_factory):
    async with pair_factory() as pair:
        pair.client._passive_commands = []
        with pytest.raises(ValueError):
            await pair.client.get_passive_connection(commands=[])


@pytest.mark.asyncio
async def test_passive_bad_choices(pair_factory):
    async with pair_factory() as pair:
        pair.server.epsv = not_implemented
        with pytest.raises(ValueError):
            await pair.client.get_passive_connection(commands=["FOO"])


@pytest.mark.parametrize("method", [("pasv", "227"), ("epsv", "229")])
@pytest.mark.asyncio
async def test_passive_multicall(pair_factory, method):
    async with pair_factory(host="127.0.0.1") as pair:
        code, info = await pair.client.command(*method)
        assert "created" in info[0]
        code, info = await pair.client.command(*method)
        assert "exists" in info[0]


@pytest.mark.parametrize("method", ["pasv", "epsv"])
@pytest.mark.asyncio
async def test_passive_closed_on_recall(pair_factory, method):
    async with pair_factory(host="127.0.0.1") as pair:
        r, w = await pair.client.get_passive_connection(commands=[method])
        nr, nw = await pair.client.get_passive_connection(commands=[method])
        with pytest.raises((ConnectionResetError, BrokenPipeError)):
            while True:
                w.write(b"-")
                await w.drain()
