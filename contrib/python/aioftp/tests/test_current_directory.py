import pathlib

import pytest

import aioftp


@pytest.mark.asyncio
async def test_current_directory_simple(pair_factory):
    async with pair_factory() as pair:
        cwd = await pair.client.get_current_directory()
        assert cwd == pathlib.PurePosixPath("/")


@pytest.mark.asyncio
async def test_current_directory_not_default(pair_factory, Server):
    s = Server([aioftp.User(home_path="/home")])
    async with pair_factory(None, s) as pair:
        cwd = await pair.client.get_current_directory()
        assert cwd == pathlib.PurePosixPath("/home")


@pytest.mark.asyncio
async def test_mlsd(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("test.txt")
        (path, stat), *_ = files = await pair.client.list()
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("test.txt")
        assert stat["type"] == "file"


@pytest.mark.asyncio
async def test_resolving_double_dots(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("test.txt")

        async def f():
            cwd = await pair.client.get_current_directory()
            assert cwd == pathlib.PurePosixPath("/")
            (path, stat), *_ = files = await pair.client.list()
            assert len(files) == 1
            assert path == pathlib.PurePosixPath("test.txt")
            assert stat["type"] == "file"

        await f()
        await pair.client.change_directory("../../../")
        await f()
        await pair.client.change_directory("/a/../b/..")
        await f()
