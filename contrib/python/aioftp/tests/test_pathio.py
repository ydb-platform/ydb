import contextlib

import pytest

import aioftp


@contextlib.contextmanager
def universal_exception_reason(*exc):
    try:
        yield
    except aioftp.PathIOError as e:
        type, instance, traceback = e.reason
        m = f"Expect one of {exc}, got {instance}"
        assert isinstance(instance, exc), m
    else:
        raise Exception(f"No excepton. Expect one of {exc}")


def test_has_state(path_io):
    assert hasattr(path_io, "state")
    path_io.state


@pytest.mark.asyncio
async def test_exists(path_io, temp_dir):
    assert await path_io.exists(temp_dir)
    assert not await path_io.exists(temp_dir / "foo")


@pytest.mark.asyncio
async def test_is_dir(path_io, temp_dir):
    assert await path_io.is_dir(temp_dir)
    p = temp_dir / "foo"
    async with path_io.open(p, mode="wb"):
        pass
    assert not await path_io.is_dir(p)


@pytest.mark.asyncio
async def test_is_file(path_io, temp_dir):
    p = temp_dir / "foo"
    assert not await path_io.is_file(temp_dir)
    async with path_io.open(p, mode="wb"):
        pass
    assert await path_io.is_file(p)


@pytest.mark.asyncio
async def test_mkdir(path_io, temp_dir):
    p = temp_dir / "foo"
    assert not await path_io.exists(p)
    await path_io.mkdir(p)
    assert await path_io.exists(p)


@pytest.mark.asyncio
async def test_rmdir(path_io, temp_dir):
    p = temp_dir / "foo"
    assert not await path_io.exists(p)
    await path_io.mkdir(p)
    assert await path_io.exists(p)
    await path_io.rmdir(p)
    assert not await path_io.exists(p)


@pytest.mark.asyncio
async def test_ulink(path_io, temp_dir):
    p = temp_dir / "foo"
    assert not await path_io.exists(p)
    async with path_io.open(p, mode="wb"):
        pass
    assert await path_io.exists(p)
    await path_io.unlink(p)
    assert not await path_io.exists(p)


@pytest.mark.asyncio
async def test_list(path_io, temp_dir):
    d, f = temp_dir / "dir", temp_dir / "file"
    await path_io.mkdir(d)
    async with path_io.open(f, mode="wb"):
        pass
    paths = await path_io.list(temp_dir)
    assert len(paths) == 2
    assert set(paths) == {d, f}
    paths = set()
    async for p in path_io.list(temp_dir):
        paths.add(p)
    assert set(paths) == {d, f}


@pytest.mark.asyncio
async def test_stat(path_io, temp_dir):
    stat = await path_io.stat(temp_dir)
    for a in ["st_size", "st_mtime", "st_ctime", "st_nlink", "st_mode"]:
        assert hasattr(stat, a)


@pytest.mark.asyncio
async def test_open_context(path_io, temp_dir):
    p = temp_dir / "context"
    async with path_io.open(p, mode="wb") as f:
        await f.write(b"foo")
    async with path_io.open(p, mode="rb") as f:
        assert await f.read() == b"foo"
    async with path_io.open(p, mode="ab") as f:
        await f.write(b"bar")
    async with path_io.open(p, mode="rb") as f:
        assert await f.read() == b"foobar"
    async with path_io.open(p, mode="wb") as f:
        await f.write(b"foo")
    async with path_io.open(p, mode="rb") as f:
        assert await f.read() == b"foo"
    async with path_io.open(p, mode="r+b") as f:
        assert await f.read(1) == b"f"
        await f.write(b"un")
    async with path_io.open(p, mode="rb") as f:
        assert await f.read() == b"fun"


@pytest.mark.asyncio
async def test_open_plain(path_io, temp_dir):
    p = temp_dir / "plain"
    f = await path_io.open(p, mode="wb")
    await f.write(b"foo")
    await f.close()
    f = await path_io.open(p, mode="rb")
    assert await f.read() == b"foo"
    await f.close()
    f = await path_io.open(p, mode="ab")
    await f.write(b"bar")
    await f.close()
    f = await path_io.open(p, mode="rb")
    assert await f.read() == b"foobar"
    await f.close()
    f = await path_io.open(p, mode="wb")
    await f.write(b"foo")
    await f.close()
    f = await path_io.open(p, mode="rb")
    assert await f.read() == b"foo"
    await f.close()
    f = await path_io.open(p, mode="r+b")
    assert await f.read(1) == b"f"
    await f.write(b"un")
    await f.close()
    f = await path_io.open(p, mode="rb")
    assert await f.read() == b"fun"
    await f.close()


@pytest.mark.asyncio
async def test_file_methods(path_io, temp_dir):
    p = temp_dir / "foo"
    async with path_io.open(p, mode="wb") as f:
        await f.write(b"foo")
        with universal_exception_reason(ValueError):
            await path_io.seek(f, 0)
        await f.seek(0)
        await f.write(b"bar")
    async with path_io.open(p, mode="rb") as f:
        assert await f.read() == b"bar"


@pytest.mark.asyncio
async def test_rename(path_io, temp_dir):
    old = temp_dir / "foo"
    new = temp_dir / "bar"
    await path_io.mkdir(old)
    assert await path_io.exists(old)
    assert not await path_io.exists(new)
    await path_io.rename(old, new)
    assert not await path_io.exists(old)
    assert await path_io.exists(new)


def test_repr_works(path_io, temp_dir):
    repr(path_io)


@pytest.mark.asyncio
async def test_path_over_file(path_io, temp_dir):
    f = temp_dir / "file"
    async with path_io.open(f, mode="wb"):
        pass
    assert not await path_io.exists(f / "dir")


@pytest.mark.asyncio
async def test_mkdir_over_file(path_io, temp_dir):
    f = temp_dir / "file"
    async with path_io.open(f, mode="wb"):
        pass
    with universal_exception_reason(FileExistsError):
        await path_io.mkdir(f)


@pytest.mark.asyncio
async def test_mkdir_no_parents(path_io, temp_dir):
    p = temp_dir / "foo" / "bar"
    with universal_exception_reason(FileNotFoundError):
        await path_io.mkdir(p)


@pytest.mark.asyncio
async def test_mkdir_parent_is_file(path_io, temp_dir):
    f = temp_dir / "foo"
    async with path_io.open(f, mode="wb"):
        pass
    with universal_exception_reason(NotADirectoryError):
        await path_io.mkdir(f / "bar")


@pytest.mark.asyncio
async def test_mkdir_parent_is_file_with_parents(path_io, temp_dir):
    f = temp_dir / "foo"
    async with path_io.open(f, mode="wb"):
        pass
    with universal_exception_reason(NotADirectoryError):
        await path_io.mkdir(f / "bar", parents=True)


@pytest.mark.asyncio
async def test_rmdir_not_exist(path_io, temp_dir):
    with universal_exception_reason(FileNotFoundError):
        await path_io.rmdir(temp_dir / "foo")


@pytest.mark.asyncio
async def test_rmdir_on_file(path_io, temp_dir):
    f = temp_dir / "foo"
    async with path_io.open(f, mode="wb"):
        pass
    with universal_exception_reason(NotADirectoryError):
        await path_io.rmdir(f)


@pytest.mark.asyncio
async def test_rmdir_not_empty(path_io, temp_dir):
    f = temp_dir / "foo"
    async with path_io.open(f, mode="wb"):
        pass
    with universal_exception_reason(OSError):
        await path_io.rmdir(temp_dir)


@pytest.mark.asyncio
async def test_unlink_not_exist(path_io, temp_dir):
    with universal_exception_reason(FileNotFoundError):
        await path_io.unlink(temp_dir / "foo")


@pytest.mark.asyncio
async def test_unlink_on_dir(path_io, temp_dir):
    with universal_exception_reason(IsADirectoryError, PermissionError):
        await path_io.unlink(temp_dir)


@pytest.mark.asyncio
async def test_list_not_exist(path_io, temp_dir):
    assert await path_io.list(temp_dir / "foo") == []


@pytest.mark.asyncio
async def test_stat_not_exist(path_io, temp_dir):
    with universal_exception_reason(FileNotFoundError):
        await path_io.stat(temp_dir / "foo")


@pytest.mark.asyncio
async def test_open_read_not_exist(path_io, temp_dir):
    with universal_exception_reason(FileNotFoundError):
        await path_io.open(temp_dir / "foo", mode="rb")


@pytest.mark.asyncio
async def test_open_write_unreachable(path_io, temp_dir):
    with universal_exception_reason(FileNotFoundError):
        await path_io.open(temp_dir / "foo" / "bar", mode="wb")


@pytest.mark.asyncio
async def test_open_write_directory(path_io, temp_dir):
    with universal_exception_reason(IsADirectoryError):
        await path_io.open(temp_dir, mode="wb")


@pytest.mark.asyncio
async def test_open_bad_mode(path_io, temp_dir):
    with universal_exception_reason(ValueError):
        await path_io.open(temp_dir, mode="bad")


@pytest.mark.asyncio
async def test_rename_source_or_dest_parent_not_exist(path_io, temp_dir):
    with universal_exception_reason(FileNotFoundError):
        await path_io.rename(temp_dir / "foo", temp_dir / "bar")
    with universal_exception_reason(FileNotFoundError):
        await path_io.rename(temp_dir, temp_dir / "foo" / "bar")


@pytest.mark.asyncio
async def test_rename_over_exists(path_io, temp_dir):
    source = temp_dir / "source"
    destination = temp_dir / "destination"
    await path_io.mkdir(source)
    await path_io.mkdir(destination)
    await path_io.rename(source, destination)
