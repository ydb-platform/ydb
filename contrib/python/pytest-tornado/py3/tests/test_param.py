import pytest
from tornado import gen

DUMMY_PARAMS = ['f00', 'bar']


@pytest.fixture(params=DUMMY_PARAMS)
def _dummy(request):
    return request.param


@pytest.mark.parametrize('input,expected', [
    ('3+5', 8),
    ('2+4', 6),
])
def test_eval(input, expected):
    assert eval(input) == expected


@pytest.mark.parametrize('input,expected', [
    ('3+5', 8),
    ('2+4', 6),
    pytest.param("6*9", 42,
                 marks=pytest.mark.xfail),
])
def test_eval_marking(input, expected):
    assert eval(input) == expected


@pytest.mark.parametrize('input,expected', [
    ('3+5', 8),
    ('2+4', 6),
])
@pytest.mark.gen_test
def test_sync_eval_with_gen_test(input, expected):
    assert eval(input) == expected


@pytest.mark.parametrize('input,expected', [
    ('3+5', 8),
    ('2+4', 6),
])
def test_eval_with_fixtures(input, io_loop, expected):
    assert eval(input) == expected


def test_param_fixture(_dummy):
    assert _dummy in DUMMY_PARAMS


@pytest.mark.gen_test
@pytest.mark.parametrize('input,expected', [
    ('3+5', 8),
    ('2+4', 6),
])
def test_gen_test_parametrize(io_loop, input, expected):
    yield gen.sleep(0)
    assert eval(input) == expected


@pytest.mark.parametrize('input,expected', [
    ('3+5', 8),
    ('2+4', 6),
])
@pytest.mark.gen_test
def test_gen_test_fixture_any_order(input, io_loop, expected):
    yield gen.sleep(0)
    assert eval(input) == expected


@pytest.mark.gen_test
def test_gen_test_param_fixture(io_loop, _dummy):
    yield gen.sleep(0)
    assert _dummy in DUMMY_PARAMS
