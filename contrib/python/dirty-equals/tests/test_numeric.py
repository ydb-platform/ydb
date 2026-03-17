import pytest

from dirty_equals import (
    IsApprox,
    IsFloat,
    IsFloatInf,
    IsFloatInfNeg,
    IsFloatInfPos,
    IsFloatNan,
    IsInt,
    IsNegative,
    IsNegativeFloat,
    IsNegativeInt,
    IsNonNegative,
    IsNonPositive,
    IsPositive,
    IsPositiveFloat,
    IsPositiveInt,
)


@pytest.mark.parametrize(
    'other,dirty',
    [
        (1, IsInt),
        (1, IsInt()),
        (1, IsInt(exactly=1)),
        (1, IsPositiveInt),
        (-1, IsNegativeInt),
        (-1.0, IsFloat),
        (-1.0, IsFloat(exactly=-1.0)),
        (1.0, IsPositiveFloat),
        (-1.0, IsNegativeFloat),
        (1, IsPositive),
        (1.0, IsPositive),
        (-1, IsNegative),
        (-1.0, IsNegative),
        (5, IsInt(gt=4)),
        (5, IsInt(ge=5)),
        (5, IsInt(lt=6)),
        (5, IsInt(le=5)),
        (1, IsApprox(1)),
        (1, IsApprox(2, delta=1)),
        (100, IsApprox(99)),
        (-100, IsApprox(-99)),
        (0, IsNonNegative),
        (1, IsNonNegative),
        (0.0, IsNonNegative),
        (1.0, IsNonNegative),
        (0, IsNonPositive),
        (-1, IsNonPositive),
        (0.0, IsNonPositive),
        (-1.0, IsNonPositive),
        (-1, IsNonPositive & IsInt),
        (1, IsNonNegative & IsInt),
        (float('inf'), IsFloatInf),
        (-float('inf'), IsFloatInf),
        (float('-inf'), IsFloatInf),
        (float('inf'), IsFloatInfPos),
        (-float('-inf'), IsFloatInfPos),
        (-float('inf'), IsFloatInfNeg),
        (float('-inf'), IsFloatInfNeg),
        (float('nan'), IsFloatNan),
        (-float('nan'), IsFloatNan),
        (float('-nan'), IsFloatNan),
    ],
)
def test_dirty_equals(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        (1.0, IsInt),
        (1.2, IsInt),
        (1, IsInt(exactly=2)),
        (True, IsInt),
        (False, IsInt),
        (1.0, IsInt()),
        (-1, IsPositiveInt),
        (0, IsPositiveInt),
        (1, IsNegativeInt),
        (0, IsNegativeInt),
        (1, IsFloat),
        (1, IsFloat(exactly=1.0)),
        (1.1234, IsFloat(exactly=1.0)),
        (-1.0, IsPositiveFloat),
        (0.0, IsPositiveFloat),
        (1.0, IsNegativeFloat),
        (0.0, IsNegativeFloat),
        (-1, IsPositive),
        (-1.0, IsPositive),
        (4, IsInt(gt=4)),
        (4, IsInt(ge=5)),
        (6, IsInt(lt=6)),
        (6, IsInt(le=5)),
        (-1, IsNonNegative),
        (-1.0, IsNonNegative),
        (1, IsNonPositive),
        (1.0, IsNonPositive),
        (-1.0, IsNonPositive & IsInt),
        (1.0, IsNonNegative & IsInt),
        (1, IsFloatNan),
        (1.0, IsFloatNan),
        (1, IsFloatInf),
        (1.0, IsFloatInf),
        (-float('inf'), IsFloatInfPos),
        (float('-inf'), IsFloatInfPos),
        (-float('-inf'), IsFloatInfNeg),
        (-float('-inf'), IsFloatInfNeg),
    ],
    ids=repr,
)
def test_dirty_not_equals(other, dirty):
    assert other != dirty


def test_invalid_approx_gt():
    with pytest.raises(TypeError, match='"approx" cannot be combined with "gt", "lt", "ge", or "le"'):
        IsInt(approx=1, gt=1)


def test_invalid_exactly_approx():
    with pytest.raises(TypeError, match='"exactly" cannot be combined with "approx"'):
        IsInt(exactly=1, approx=1)


def test_invalid_exactly_gt():
    with pytest.raises(TypeError, match='"exactly" cannot be combined with "gt", "lt", "ge", or "le"'):
        IsInt(exactly=1, gt=1)


def test_not_int():
    d = IsInt()
    with pytest.raises(AssertionError):
        assert '1' == d
    assert repr(d) == 'IsInt()'


def test_not_negative():
    d = IsNegativeInt
    with pytest.raises(AssertionError):
        assert 1 == d
    assert repr(d) == 'IsNegativeInt'
