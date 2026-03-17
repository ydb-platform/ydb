from thinc.api import (
    compounding,
    constant,
    constant_then,
    cyclic_triangular,
    decaying,
    slanted_triangular,
    warmup_linear,
)


def test_decaying_rate():
    rates = decaying(0.001, 1e-4)
    rate = next(rates)
    assert rate == 0.001
    next_rate = next(rates)
    assert next_rate < rate
    assert next_rate > 0
    assert next_rate > next(rates)


def test_compounding_rate():
    rates = compounding(1, 16, 1.01)
    rate0 = next(rates)
    assert rate0 == 1.0
    rate1 = next(rates)
    rate2 = next(rates)
    rate3 = next(rates)
    assert rate3 > rate2 > rate1 > rate0
    assert (rate3 - rate2) > (rate2 - rate1) > (rate1 - rate0)


def test_slanted_triangular_rate():
    rates = slanted_triangular(1.0, 20.0, ratio=10)
    rate0 = next(rates)
    assert rate0 < 1.0
    rate1 = next(rates)
    assert rate1 > rate0
    rate2 = next(rates)
    assert rate2 < rate1
    rate3 = next(rates)
    assert rate0 < rate3 < rate2


def test_constant_then_schedule():
    rates = constant_then(1.0, 2, [100, 200])
    assert next(rates) == 1.0
    assert next(rates) == 1.0
    assert next(rates) == 100
    assert next(rates) == 200


def test_constant():
    rates = constant(123)
    assert next(rates) == 123
    assert next(rates) == 123


def test_warmup_linear():
    rates = warmup_linear(1.0, 2, 10)
    expected = [0.0, 0.5, 1.0, 0.875, 0.75, 0.625, 0.5, 0.375, 0.25, 0.125, 0.0]
    for i in range(11):
        assert next(rates) == expected[i]


def test_cyclic_triangular():
    rates = cyclic_triangular(0.1, 1.0, 2)
    expected = [0.55, 1.0, 0.55, 0.1, 0.55, 1.0, 0.55, 0.1, 0.55, 1.0]
    for i in range(10):
        assert next(rates) == expected[i]
