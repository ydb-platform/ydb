import contextlib
import pathlib
import textwrap

import pytest

import aioftp


async def not_implemented(connection, rest):
    connection.response("502", ":P")
    return True


@pytest.mark.asyncio
async def test_client_fallback_to_list_at_list(pair_factory):
    async with pair_factory() as pair:
        pair.server.commands_mapping["mlst"] = not_implemented
        pair.server.commands_mapping["mlsd"] = not_implemented
        await pair.make_server_files("bar/foo")
        (path, stat), *_ = files = await pair.client.list()
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("bar")
        assert stat["type"] == "dir"
        (path, stat), *_ = files = await pair.client.list("bar")
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("bar/foo")
        assert stat["type"] == "file"
        result = await pair.client.list("bar/foo")
        assert len(result) == 0


async def implemented_badly(connection, rest):
    assert False, "should not be called"


@pytest.mark.asyncio
async def test_client_list_override(pair_factory):
    async with pair_factory() as pair:
        pair.server.commands_mapping["mlsd"] = implemented_badly
        await pair.client.make_directory("bar")
        (path, stat), *_ = files = await pair.client.list(raw_command="LIST")
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("bar")
        assert stat["type"] == "dir"


@pytest.mark.asyncio
async def test_client_list_override_invalid_raw_command(pair_factory):
    async with pair_factory() as pair:
        with pytest.raises(ValueError):
            await pair.client.list(raw_command="FOO")


def test_client_list_windows():
    test_str = textwrap.dedent(
        """\
         11/4/2018   9:09 PM  <DIR>         .
         8/10/2018   1:02 PM  <DIR>         ..
         9/23/2018   2:16 PM  <DIR>         bin
        10/16/2018  10:25 PM  <DIR>         Desktop
         11/4/2018   3:31 PM  <DIR>         dow
        10/16/2018   8:21 PM  <DIR>         Downloads
        10/14/2018   5:34 PM  <DIR>         msc
          9/9/2018   9:32 AM  <DIR>         opt
         10/3/2018   2:58 PM    34,359,738,368  win10.img
         6/30/2018   8:36 AM    3,939,237,888  win10.iso
         7/26/2018   1:11 PM           189  win10.sh
        10/29/2018  11:46 AM    34,359,738,368  win7.img
         6/30/2018   8:35 AM    3,319,791,616  win7.iso
        10/29/2018  10:55 AM           219  win7.sh
               6 files           75,978,506,648 bytes
               3 directories     22,198,362,112 bytes free
    """,
    )
    test_str = test_str.strip().split("\n")
    entities = {}
    parse = aioftp.Client(encoding="utf-8").parse_list_line_windows
    for x in test_str:
        with contextlib.suppress(ValueError):
            path, stat = parse(x.encode("utf-8"))
            entities[path] = stat
    dirs = ["bin", "Desktop", "dow", "Downloads", "msc", "opt"]
    files = [
        "win10.img",
        "win10.iso",
        "win10.sh",
        "win7.img",
        "win7.iso",
        "win7.sh",
    ]
    assert len(entities) == len(dirs + files)
    for d in dirs:
        p = pathlib.PurePosixPath(d)
        assert p in entities
        assert entities[p]["type"] == "dir"
    for f in files:
        p = pathlib.PurePosixPath(f)
        assert p in entities
        assert entities[p]["type"] == "file"
    with pytest.raises(ValueError):
        parse(b" 10/3/2018   2:58 PM    34,35xxx38,368  win10.img")


@pytest.mark.asyncio
async def test_client_list_override_with_custom(pair_factory, Client):
    meta = {"type": "file", "works": True}

    def parser(b):
        import pickle

        return pickle.loads(bytes.fromhex(b.decode().rstrip("\r\n")))

    async def builder(_, path):
        import pickle

        return pickle.dumps((path, meta)).hex()

    async with pair_factory(Client(parse_list_line_custom=parser)) as pair:
        pair.server.commands_mapping["mlst"] = not_implemented
        pair.server.commands_mapping["mlsd"] = not_implemented
        pair.server.build_list_string = builder
        await pair.client.make_directory("bar")
        (path, stat), *_ = files = await pair.client.list()
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("bar")
        assert stat == meta


@pytest.mark.asyncio
async def test_client_list_override_with_custom_last(pair_factory, Client):
    meta = {"type": "file", "works": True}

    def parser(b):
        import pickle

        return pickle.loads(bytes.fromhex(b.decode().rstrip("\r\n")))

    async def builder(_, path):
        import pickle

        return pickle.dumps((path, meta)).hex()

    client = Client(
        parse_list_line_custom=parser,
        parse_list_line_custom_first=False,
    )
    async with pair_factory(client) as pair:
        pair.server.commands_mapping["mlst"] = not_implemented
        pair.server.commands_mapping["mlsd"] = not_implemented
        pair.server.build_list_string = builder
        await pair.client.make_directory("bar")
        (path, stat), *_ = files = await pair.client.list()
        assert len(files) == 1
        assert path == pathlib.PurePosixPath("bar")
        assert stat == meta
