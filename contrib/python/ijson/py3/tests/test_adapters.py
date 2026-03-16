import asyncio
import ijson
import pytest


def test_from_iter_read0_does_not_consume():
    chunks = [b'{"key":', b'"value"}']
    file_obj = ijson.from_iter(iter(chunks))
    assert file_obj.read(0) == b""
    assert file_obj.read(1) == b'{"key":'
    assert file_obj.read(1) == b'"value"}'
    assert file_obj.read(1) == b""


@pytest.mark.parametrize(
    "chunks_factory",
    [list, tuple, iter, lambda chunks: (chunk for chunk in chunks)],
    ids=["list", "tuple", "iter", "generator"],
)
def test_from_iter_accepts_iterable(chunks_factory):
    chunks = chunks_factory([b'{"key":', b'"value"}'])
    file_obj = ijson.from_iter(chunks)
    assert file_obj.read(1) == b'{"key":'
    assert file_obj.read(1) == b'"value"}'
    assert file_obj.read(1) == b""


@pytest.mark.parametrize(
    "chunks_factory",
    [list, tuple, iter, lambda chunks: (chunk for chunk in chunks)],
    ids=["list", "tuple", "iter", "generator"],
)
def test_from_iter_accepts_string_and_warns(chunks_factory):
    chunks = chunks_factory(['{"key":', '"value"}'])
    file_obj = ijson.from_iter(chunks)
    with pytest.deprecated_call():
        assert file_obj.read(1) == b'{"key":'
    assert file_obj.read(1) == b'"value"}'
    assert file_obj.read(1) == b""


def test_from_iter_accepts_aiterable():
    async def chunks():
        yield b'{"key":'
        yield b'"value"}'

    async def main():
        file_obj = ijson.from_iter(chunks())
        assert await file_obj.read(0) == b""
        assert await file_obj.read(1) == b'{"key":'
        assert await file_obj.read(1) == b'"value"}'
        assert await file_obj.read(1) == b""

    asyncio.run(main())


def test_from_iter_accepts_async_string_chunks_and_warns():
    async def chunks():
        yield '{"key":'
        yield '"value"}'

    async def main():
        file_obj = ijson.from_iter(chunks())
        with pytest.deprecated_call():
            assert await file_obj.read(1) == b'{"key":'
        assert await file_obj.read(1) == b'"value"}'
        assert await file_obj.read(1) == b""

    asyncio.run(main())
