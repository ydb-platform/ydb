import pytest

from thinc.api import (
    ParametricAttention,
    Relu,
    Softmax,
    chain,
    list2ragged,
    reduce_max,
    reduce_sum,
    with_ragged,
)
from thinc.util import DataValidationError, data_validation


@pytest.mark.xfail(reason="Validation currently disabled for Pydantic 2 changes0")
def test_validation():
    model = chain(Relu(10), Relu(10), with_ragged(reduce_max()), Softmax())
    with data_validation(True):
        with pytest.raises(DataValidationError):
            model.initialize(X=model.ops.alloc2f(1, 10), Y=model.ops.alloc2f(1, 10))
        with pytest.raises(DataValidationError):
            model.initialize(X=model.ops.alloc3f(1, 10, 1), Y=model.ops.alloc2f(1, 10))
        with pytest.raises(DataValidationError):
            model.initialize(X=[model.ops.alloc2f(1, 10)], Y=model.ops.alloc2f(1, 10))


@pytest.mark.xfail(reason="Validation currently disabled for Pydantic 2 changes0")
def test_validation_complex():
    good_model = chain(list2ragged(), reduce_sum(), Relu(12, dropout=0.5), Relu(1))
    X = [good_model.ops.xp.zeros((4, 75), dtype="f")]
    Y = good_model.ops.xp.zeros((1,), dtype="f")
    good_model.initialize(X, Y)
    good_model.predict(X)

    bad_model = chain(
        list2ragged(),
        reduce_sum(),
        Relu(12, dropout=0.5),
        # ERROR: Why can't I attach a Relu to an attention layer?
        ParametricAttention(12),
        Relu(1),
    )
    with data_validation(True):
        with pytest.raises(DataValidationError):
            bad_model.initialize(X, Y)
