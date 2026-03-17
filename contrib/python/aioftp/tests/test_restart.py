import pytest


@pytest.mark.parametrize("offset", [0, 3, 10])
@pytest.mark.asyncio
async def test_restart_retr(pair_factory, offset):
    async with pair_factory() as pair:
        atom = b"foobar"
        name = "foo.txt"
        await pair.make_server_files(name, size=1, atom=atom)
        async with pair.client.download_stream(name, offset=offset) as stream:
            assert await stream.read() == atom[offset:]


@pytest.mark.parametrize("offset", [1, 3, 10])
@pytest.mark.parametrize("method", ["upload_stream", "append_stream"])
@pytest.mark.asyncio
async def test_restart_stor_appe(pair_factory, offset, method):
    async with pair_factory() as pair:
        atom = b"foobar"
        name = "foo.txt"
        insert = b"123"
        expect = atom[:offset] + b"\x00" * (offset - len(atom)) + insert + atom[offset + len(insert) :]
        await pair.make_server_files(name, size=1, atom=atom)
        stream_factory = getattr(pair.client, method)
        async with stream_factory(name, offset=offset) as stream:
            await stream.write(b"123")
        async with pair.client.download_stream(name) as stream:
            assert await stream.read() == expect


@pytest.mark.asyncio
async def test_restart_reset(pair_factory):
    async with pair_factory() as pair:
        atom = b"foobar"
        name = "foo.txt"
        await pair.make_server_files(name, size=1, atom=atom)
        await pair.client.command("REST 3", "350")
        async with pair.client.download_stream("foo.txt") as stream:
            assert await stream.read() == atom


@pytest.mark.asyncio
async def test_restart_syntax_error(pair_factory, expect_codes_in_exception):
    async with pair_factory() as pair:
        with expect_codes_in_exception("501"):
            await pair.client.command("REST 3abc", "350")
