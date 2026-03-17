import pytest

import aioftp


@pytest.mark.asyncio
async def test_stream_iter_by_line(pair_factory):
    async with pair_factory() as pair:
        await pair.client.make_directory("bar")
        lines = []
        async with pair.client.get_stream("list") as stream:
            async for line in stream.iter_by_line():
                lines.append(line)
        assert len(lines) == 1
        assert b"bar" in lines[0]


@pytest.mark.asyncio
async def test_stream_close_without_finish(pair_factory):
    class CustomException(Exception):
        pass

    def fake_finish(*a, **kw):
        raise Exception("Finished called")

    async with pair_factory() as pair:
        with pytest.raises(CustomException):
            async with pair.client.get_stream():
                raise CustomException()


@pytest.mark.asyncio
async def test_no_server(unused_tcp_port):
    with pytest.raises(OSError):
        async with aioftp.Client.context("127.0.0.1", unused_tcp_port):
            pass


@pytest.mark.asyncio
async def test_syst_command(pair_factory):
    async with pair_factory() as pair:
        code, info = await pair.client.command("syst", "215")
        assert info == [" UNIX Type: L8"]


@pytest.mark.asyncio
async def test_illegal_command(pair_factory):
    async with pair_factory() as pair:
        with pytest.raises(aioftp.errors.InvalidCommand):
            await pair.client.command("LIST\r\nfoo")
