import functools
import pytest
import tornado
from tornado import gen
from tornado.ioloop import TimeoutError


@gen.coroutine
def dummy_coroutine(io_loop):
    yield gen.sleep(0)
    raise gen.Return(True)


def test_explicit_start_and_stop(io_loop):
    future = dummy_coroutine(io_loop)
    future.add_done_callback(lambda *args: io_loop.stop())
    io_loop.start()
    assert future.result()


def test_run_sync(io_loop):
    dummy = functools.partial(dummy_coroutine, io_loop)
    finished = io_loop.run_sync(dummy)
    assert finished


@pytest.mark.gen_test
def test_gen_test_sync(io_loop):
    assert True


@pytest.mark.gen_test
def test_gen_test(io_loop):
    result = yield dummy_coroutine(io_loop)
    assert result


@pytest.mark.gen_test(run_sync=False)
def test_gen_test_run_sync_false(io_loop):
    result = yield dummy_coroutine(io_loop)
    assert result


@pytest.mark.gen_test
def test_gen_test_swallows_exceptions(io_loop):
    with pytest.raises(ZeroDivisionError):
        1 / 0


@pytest.mark.gen_test
def test_generator_raises(io_loop):
    with pytest.raises(ZeroDivisionError):
        yield gen.sleep(0)
        1 / 0


@pytest.mark.gen_test
def test_explicit_gen_test_marker(request, io_loop):
    yield gen.sleep(0)
    assert 'gen_test' in request.keywords


@pytest.mark.gen_test(timeout=2.1)
def test_gen_test_marker_with_params(request, io_loop):
    yield gen.sleep(0)
    assert request.node.get_closest_marker('gen_test').kwargs['timeout'] == 2.1


@pytest.mark.xfail(raises=TimeoutError)
@pytest.mark.gen_test(timeout=0.1)
def test_gen_test_with_timeout(io_loop):
    yield gen.sleep(1)


def test_sync_tests_no_gen_test_marker(request):
    assert 'gen_test' not in request.keywords


class TestClass:
    @pytest.mark.gen_test
    def test_gen_test(self, io_loop):
        result = yield dummy_coroutine(io_loop)
        assert result

    @pytest.mark.gen_test
    def test_generator_raises(self, io_loop):
        with pytest.raises(ZeroDivisionError):
            yield gen.sleep(0)
            1 / 0
