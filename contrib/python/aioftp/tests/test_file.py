import datetime as dt
import math
from pathlib import PurePosixPath

import pytest

import aioftp


@pytest.mark.asyncio
async def test_remove_single_file(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("foo.txt")
        assert await pair.server_paths_exists("foo.txt")
        await pair.client.remove_file("foo.txt")
        assert await pair.server_paths_exists("foo.txt") is False


@pytest.mark.asyncio
async def test_recursive_remove(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar.txt", "foo/baz.txt", "foo/bar_dir/foo.baz"]
        await pair.make_server_files(*paths)
        await pair.client.remove("foo")
        assert await pair.server_paths_exists(*paths) is False


@pytest.mark.asyncio
async def test_mlsd_file(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("foo/bar.txt")
        result = await pair.client.list("foo/bar.txt")
        assert len(result) == 0


@pytest.mark.asyncio
async def test_file_download(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("foo", size=1, atom=b"foobar")
        async with pair.client.download_stream("foo") as stream:
            data = await stream.read()
        assert data == b"foobar"


@pytest.mark.asyncio
async def test_file_download_exactly(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("foo", size=1, atom=b"foobar")
        async with pair.client.download_stream("foo") as stream:
            data1 = await stream.readexactly(3)
            data2 = await stream.readexactly(3)
        assert (data1, data2) == (b"foo", b"bar")


@pytest.mark.asyncio
async def test_file_download_enhanced_passive(pair_factory):
    async with pair_factory() as pair:
        pair.client._passive_commands = ["epsv"]
        await pair.make_server_files("foo", size=1, atom=b"foobar")
        async with pair.client.download_stream("foo") as stream:
            data = await stream.read()
        assert data == b"foobar"


@pytest.mark.asyncio
async def test_file_upload(pair_factory):
    async with pair_factory() as pair:
        async with pair.client.upload_stream("foo") as stream:
            await stream.write(b"foobar")
        async with pair.client.download_stream("foo") as stream:
            data = await stream.read()
        assert data == b"foobar"


@pytest.mark.asyncio
async def test_file_append(pair_factory):
    async with pair_factory() as pair:
        await pair.make_server_files("foo", size=1, atom=b"foobar")
        async with pair.client.append_stream("foo") as stream:
            await stream.write(b"foobar")
        async with pair.client.download_stream("foo") as stream:
            data = await stream.read()
        assert data == b"foobar" * 2


@pytest.mark.asyncio
async def test_upload_folder(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar", "foo/baz"]
        await pair.make_client_files(*paths)
        assert await pair.server_paths_exists(*paths) is False
        await pair.client.upload("foo")
        assert await pair.server_paths_exists(*paths)


@pytest.mark.asyncio
async def test_upload_folder_into(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar", "foo/baz"]
        await pair.make_client_files(*paths)
        assert await pair.server_paths_exists("bar", "baz") is False
        await pair.client.upload("foo", write_into=True)
        assert await pair.server_paths_exists("bar", "baz")


@pytest.mark.asyncio
async def test_upload_folder_into_another(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar", "foo/baz"]
        await pair.make_client_files(*paths)
        assert await pair.server_paths_exists("bar/bar", "bar/baz") is False
        await pair.client.upload("foo", "bar", write_into=True)
        assert await pair.server_paths_exists("bar/bar", "bar/baz")


@pytest.mark.asyncio
async def test_download_folder(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar", "foo/baz"]
        await pair.make_server_files(*paths)
        assert await pair.client_paths_exists(*paths) is False
        await pair.client.download("foo")
        assert await pair.client_paths_exists(*paths)


@pytest.mark.asyncio
async def test_download_folder_into(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar", "foo/baz"]
        await pair.make_server_files(*paths)
        assert await pair.client_paths_exists("bar", "baz") is False
        await pair.client.download("foo", write_into=True)
        assert await pair.client_paths_exists("bar", "baz")


@pytest.mark.asyncio
async def test_download_folder_into_another(pair_factory):
    async with pair_factory() as pair:
        paths = ["foo/bar", "foo/baz"]
        await pair.make_server_files(*paths)
        assert await pair.client_paths_exists("bar/bar", "bar/baz") is False
        await pair.client.download("foo", "bar", write_into=True)
        assert await pair.client_paths_exists("bar/bar", "bar/baz")


@pytest.mark.asyncio
async def test_upload_file_over(pair_factory):
    async with pair_factory() as pair:
        await pair.make_client_files("foo", size=1, atom=b"client")
        await pair.make_server_files("foo", size=1, atom=b"server")
        async with pair.client.download_stream("foo") as stream:
            assert await stream.read() == b"server"
        await pair.client.upload("foo")
        async with pair.client.download_stream("foo") as stream:
            assert await stream.read() == b"client"


@pytest.mark.asyncio
async def test_download_file_over(pair_factory):
    async with pair_factory() as pair:
        await pair.make_client_files("foo", size=1, atom=b"client")
        await pair.make_server_files("foo", size=1, atom=b"server")
        async with pair.client.path_io.open(PurePosixPath("foo")) as f:
            assert await f.read() == b"client"
        await pair.client.download("foo")
        async with pair.client.path_io.open(PurePosixPath("foo")) as f:
            assert await f.read() == b"server"


@pytest.mark.asyncio
async def test_upload_file_write_into(pair_factory):
    async with pair_factory() as pair:
        await pair.make_client_files("foo", size=1, atom=b"client")
        await pair.make_server_files("bar", size=1, atom=b"server")
        async with pair.client.download_stream("bar") as stream:
            assert await stream.read() == b"server"
        await pair.client.upload("foo", "bar", write_into=True)
        async with pair.client.download_stream("bar") as stream:
            assert await stream.read() == b"client"


@pytest.mark.asyncio
async def test_upload_tree(pair_factory):
    async with pair_factory() as pair:
        await pair.make_client_files("foo/bar/baz", size=1, atom=b"client")
        await pair.client.upload("foo", "bar", write_into=True)
        files = await pair.client.list(recursive=True)
        assert len(files) == 3


@pytest.mark.asyncio
async def test_download_file_write_into(pair_factory):
    async with pair_factory() as pair:
        await pair.make_client_files("foo", size=1, atom=b"client")
        await pair.make_server_files("bar", size=1, atom=b"server")
        async with pair.client.path_io.open(PurePosixPath("foo")) as f:
            assert await f.read() == b"client"
        await pair.client.download("bar", "foo", write_into=True)
        async with pair.client.path_io.open(PurePosixPath("foo")) as f:
            assert await f.read() == b"server"


@pytest.mark.asyncio
async def test_upload_file_os_error(
    pair_factory,
    Server,
    expect_codes_in_exception,
):
    class OsErrorPathIO(aioftp.MemoryPathIO):
        @aioftp.pathio.universal_exception
        async def write(self, fout, data):
            raise OSError("test os error")

    s = Server(path_io_factory=OsErrorPathIO)
    async with pair_factory(None, s) as pair:
        with expect_codes_in_exception("451"):
            async with pair.client.upload_stream("foo") as stream:
                await stream.write(b"foobar")


@pytest.mark.asyncio
async def test_upload_path_unreachable(
    pair_factory,
    expect_codes_in_exception,
):
    async with pair_factory() as pair:
        with expect_codes_in_exception("550"):
            async with pair.client.upload_stream("foo/bar/foo") as stream:
                await stream.write(b"foobar")


@pytest.mark.asyncio
async def test_stat_when_no_mlst(pair_factory):
    async with pair_factory() as pair:
        pair.server.commands_mapping.pop("mlst")
        await pair.make_server_files("foo")
        info = await pair.client.stat("foo")
        assert info["type"] == "file"


@pytest.mark.asyncio
async def test_stat_mlst(pair_factory):
    async with pair_factory() as pair:
        now = dt.datetime.now(tz=dt.timezone.utc).replace(tzinfo=None)
        await pair.make_server_files("foo")
        info = await pair.client.stat("foo")
        assert info["type"] == "file"
        for fact in ("modify", "create"):
            received = dt.datetime.strptime(info[fact], "%Y%m%d%H%M%S")
            assert math.isclose(
                now.timestamp(),
                received.timestamp(),
                abs_tol=10,
            )
