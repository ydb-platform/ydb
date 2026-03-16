import pytest

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
