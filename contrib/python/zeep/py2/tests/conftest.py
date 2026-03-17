import sys

import pytest

# Don't try to test asyncio since it is py3 only syntax
if sys.version_info < (3, 5):
    collect_ignore = ["test_asyncio_transport.py"]

pytest.register_assert_rewrite("tests.utils")


@pytest.fixture(autouse=True)
def no_requests(request, monkeypatch):
    if request.node.get_closest_marker("requests"):
        return

    def func(*args, **kwargs):
        pytest.fail("External connections not allowed during tests.")

    monkeypatch.setattr("socket.socket", func)


@pytest.yield_fixture()
def event_loop():
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    yield loop
    loop.close()
