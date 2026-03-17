from typing import List

from thinc.api import Model, with_flatten_v2

INPUT = [[1, 2, 3], [4, 5], [], [6, 7, 8]]
INPUT_FLAT = [1, 2, 3, 4, 5, 6, 7, 8]
OUTPUT = [[2, 3, 4], [5, 6], [], [7, 8, 9]]
BACKPROP_OUTPUT = [[3, 4, 5], [6, 7], [], [8, 9, 10]]


def _memoize_input() -> Model[List[int], List[int]]:
    return Model(name="memoize_input", forward=_memoize_input_forward)


def _memoize_input_forward(
    model: Model[List[int], List[int]], X: List[int], is_train: bool
):
    model.attrs["last_input"] = X

    def backprop(dY: List[int]):
        return [v + 2 for v in dY]

    return [v + 1 for v in X], backprop


def test_with_flatten():
    model = with_flatten_v2(_memoize_input())
    Y, backprop = model(INPUT, is_train=True)
    assert Y == OUTPUT
    assert model.layers[0].attrs["last_input"] == INPUT_FLAT
    assert backprop(INPUT) == BACKPROP_OUTPUT
