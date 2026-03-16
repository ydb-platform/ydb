from pathlib import Path

import pytest

import aioftp


@pytest.mark.asyncio
async def test_server_side_exception(pair_factory):
    class CustomServer(aioftp.Server):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.commands_mapping["custom"] = self.custom

        async def custom(*args, **kwargs):
            raise RuntimeError("Test error")

    factory = pair_factory(server_factory=CustomServer, do_quit=False)
    async with factory as pair:
        with pytest.raises(ConnectionResetError):
            await pair.client.command("custom", "200")


@pytest.mark.asyncio
async def test_bad_type_value(pair_factory, expect_codes_in_exception):
    async with pair_factory() as pair:
        with expect_codes_in_exception("502"):
            await pair.client.command("type FOO", "200")


@pytest.mark.asyncio
async def test_pbsz(pair_factory):
    async with pair_factory() as pair:
        await pair.client.command("pbsz", "200")


@pytest.mark.asyncio
async def test_prot(pair_factory, expect_codes_in_exception):
    async with pair_factory() as pair:
        await pair.client.command("prot P", "200")
        with expect_codes_in_exception("502"):
            await pair.client.command("prot foo", "200")


@pytest.mark.asyncio
async def test_server_ipv6_pasv(pair_factory, expect_codes_in_exception):
    async with pair_factory(host="::1", do_quit=False) as pair:
        with expect_codes_in_exception("503"):
            await pair.client.get_passive_connection(commands=["pasv"])


@pytest.mark.asyncio
async def test_epsv_extra_arg(pair_factory, expect_codes_in_exception):
    async with pair_factory(do_quit=False) as pair:
        with expect_codes_in_exception("522"):
            await pair.client.command("epsv foo", "229")


@pytest.mark.asyncio
async def test_bad_server_path_io(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    class BadPathIO(aioftp.MemoryPathIO):
        async def is_file(*a, **kw):
            return False

        async def is_dir(*a, **kw):
            return False

    s = Server(path_io_factory=BadPathIO)
    async with pair_factory(None, s) as pair:
        pio = pair.server.path_io_factory()
        async with pio.open(Path("/foo"), "wb"):
            pass
        await pair.client.list()
