import numpy
import pytest

from thinc.api import NumpyOps, Ragged, registry, strings2arrays

from ..util import get_data_checker


@pytest.fixture(params=[[], [(10, 2)], [(5, 3), (1, 3)], [(2, 3), (0, 3), (1, 3)]])
def shapes(request):
    return request.param


@pytest.fixture
def ops():
    return NumpyOps()


@pytest.fixture
def list_data(shapes):
    return [numpy.zeros(shape, dtype="f") for shape in shapes]


@pytest.fixture
def ragged_data(ops, list_data):
    lengths = numpy.array([len(x) for x in list_data], dtype="i")
    if not list_data:
        return Ragged(ops.alloc2f(0, 0), lengths)
    else:
        return Ragged(ops.flatten(list_data), lengths)


@pytest.fixture
def padded_data(ops, list_data):
    return ops.list2padded(list_data)


@pytest.fixture
def array_data(ragged_data):
    return ragged_data.data


def check_transform(transform, in_data, out_data):
    model = registry.resolve({"config": {"@layers": transform}})["config"]
    input_checker = get_data_checker(in_data)
    output_checker = get_data_checker(out_data)
    model.initialize(in_data, out_data)
    Y, backprop = model(in_data, is_train=True)
    output_checker(Y, out_data)
    dX = backprop(Y)
    input_checker(dX, in_data)


def test_list2array(list_data, array_data):
    check_transform("list2array.v1", list_data, array_data)


def test_list2ragged(list_data, ragged_data):
    check_transform("list2ragged.v1", list_data, ragged_data)


def test_list2padded(list_data, padded_data):
    check_transform("list2padded.v1", list_data, padded_data)


def test_ragged2list(ragged_data, list_data):
    check_transform("ragged2list.v1", ragged_data, list_data)


def test_padded2list(padded_data, list_data):
    check_transform("padded2list.v1", padded_data, list_data)


def test_strings2arrays():
    strings = ["hello", "world"]
    model = strings2arrays()
    Y, backprop = model.begin_update(strings)
    assert len(Y) == len(strings)
    assert backprop([]) == []
