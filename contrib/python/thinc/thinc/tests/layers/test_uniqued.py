import numpy
import pytest
from hypothesis import given, settings
from hypothesis.strategies import composite, integers, lists
from numpy.testing import assert_allclose

from thinc.layers import Embed
from thinc.layers.uniqued import uniqued

ROWS = 10

# This test uses a newer hypothesis feature than the skanky flatmap-style
# I used previously. This is much nicer, although it still takes some getting
# used to. The key feature is this composite decorator. It injects a function,
# 'draw'.


@composite
def lists_of_integers(draw, columns=2, lo=0, hi=ROWS - 1):
    # We call draw to get example values, which we can manipulate.
    # Here we get a list of integers, where each member of the list
    # should be between a min and max value.
    int_list = draw(lists(integers(min_value=lo, max_value=hi)))
    # Now we can use this int list to make an array, and it'll be the arrays
    # that our functions receive.
    # We trim the list, so we're of length divisible by columns.
    int_list = int_list[len(int_list) % columns :]
    # And make the array and reshape it.
    array = numpy.array(int_list, dtype="uint64")
    return array.reshape((-1, columns))


@pytest.fixture(scope="module")
def model(nO=128):
    return Embed(nO, ROWS, column=0).initialize()


def test_uniqued_calls_init():
    calls = []
    embed = Embed(5, 5, column=0)
    embed.init = lambda *args, **kwargs: calls.append(True)
    embed.initialize()
    assert calls == [True]
    uembed = uniqued(embed)
    uembed.initialize()
    assert calls == [True, True]


@given(X=lists_of_integers(lo=0, hi=ROWS - 1))
@settings(deadline=None)
def test_uniqued_doesnt_change_result(model, X):
    umodel = uniqued(model, column=model.attrs["column"]).initialize()
    Y, bp_Y = model(X, is_train=True)
    Yu, bp_Yu = umodel(X, is_train=True)
    assert_allclose(Y, Yu)
    dX = bp_Y(Y)
    dXu = bp_Yu(Yu)
    assert_allclose(dX, dXu)
    if X.size:
        pass
        # TODO: This test is a problem, because we exceed the embedding table.
        # Fix it with a better cap.
        # Check that different inputs do give different results
        # Z, bp_Z = model(X + 1, is_train=True)
        # with pytest.raises(AssertionError):
        #    assert_allclose(Y, Z)
