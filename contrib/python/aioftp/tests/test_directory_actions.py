import pathlib

import pytest

import aioftp


@pytest.mark.asyncio
async def test_create_and_remove_directory(pair_factory):
    async with pair_factory() as pair:
        await pair.client.make_directory("bar")
        (path, stat), *_ = files = await pair.client.list()
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("bar")
        assert stat["type"] == "dir"

        await pair.client.remove_directory("bar")
        assert await pair.server_paths_exists("bar") is False


@pytest.mark.asyncio
async def test_create_and_remove_directory_long(pair_factory):
    async with pair_factory() as pair:
        await pair.client.make_directory("bar/baz")
        assert await pair.server_paths_exists("bar", "bar/baz")
        await pair.client.remove_directory("bar/baz")
        await pair.client.remove_directory("bar")
        assert await pair.server_paths_exists("bar") is False


@pytest.mark.asyncio
async def test_create_directory_long_no_parents(pair_factory):
    async with pair_factory() as pair:
        await pair.client.make_directory("bar/baz", parents=False)
        await pair.client.remove_directory("bar/baz")
        await pair.client.remove_directory("bar")


@pytest.mark.asyncio
async def test_change_directory(pair_factory):
    async with pair_factory() as pair:
        await pair.client.make_directory("bar")
        await pair.client.change_directory("bar")
        cwd = await pair.client.get_current_directory()
        assert cwd == pathlib.PurePosixPath("/bar")
        await pair.client.change_directory()
        cwd = await pair.client.get_current_directory()
        assert cwd == pathlib.PurePosixPath("/")


@pytest.mark.asyncio
async def test_change_directory_not_exist(
    pair_factory,
    expect_codes_in_exception,
):
    async with pair_factory() as pair:
        with expect_codes_in_exception("550"):
            await pair.client.change_directory("bar")


@pytest.mark.asyncio
async def test_rename_empty_directory(pair_factory):
    async with pair_factory() as pair:
        await pair.client.make_directory("bar")
        assert await pair.server_paths_exists("bar")
        assert await pair.server_paths_exists("baz") is False
        await pair.client.rename("bar", "baz")
        assert await pair.server_paths_exists("bar") is False
        assert await pair.server_paths_exists("baz")


@pytest.mark.asyncio
async def test_rename_non_empty_directory(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("bar/foo.txt")
        assert await pair.server_paths_exists("bar/foo.txt", "bar")
        await pair.client.make_directory("hurr")
        await pair.client.rename("bar", "hurr/baz")
        assert await pair.server_paths_exists("hurr/baz/foo.txt")
        assert await pair.server_paths_exists("bar") is False


class FakeErrorPathIO(aioftp.MemoryPathIO):
    def list(self, path):
        class Lister(aioftp.AbstractAsyncLister):
            @aioftp.pathio.universal_exception
            async def __anext__(self):
                raise Exception("KERNEL PANIC")

        return Lister(timeout=self.timeout)


@pytest.mark.asyncio
async def test_exception_in_list(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    s = Server(path_io_factory=FakeErrorPathIO)
    async with pair_factory(None, s) as pair:
        with expect_codes_in_exception("451"):
            await pair.client.list()


@pytest.mark.asyncio
async def test_list_recursive(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("foo/bar", "foo/baz/baz")
        files = await pair.client.list(recursive=True)
        assert len(files) == 4
