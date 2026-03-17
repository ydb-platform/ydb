from typing import cast

import numpy
import pytest

from thinc.api import (
    Adam,
    ArgsKwargs,
    Model,
    MXNetWrapper,
    Ops,
    get_current_ops,
    mxnet2xp,
    xp2mxnet,
)
from thinc.compat import has_cupy_gpu, has_mxnet
from thinc.types import Array1d, Array2d, IntsXd
from thinc.util import to_categorical

from ..util import check_input_converters, make_tempdir


@pytest.fixture
def n_hidden() -> int:
    return 12


@pytest.fixture
def input_size() -> int:
    return 784


@pytest.fixture
def n_classes() -> int:
    return 10


@pytest.fixture
def answer() -> int:
    return 1


@pytest.fixture
def X(input_size: int) -> Array2d:
    ops: Ops = get_current_ops()
    return cast(Array2d, ops.alloc(shape=(1, input_size)))


@pytest.fixture
def Y(answer: int, n_classes: int) -> Array2d:
    ops: Ops = get_current_ops()
    return cast(
        Array2d,
        to_categorical(cast(IntsXd, ops.asarray([answer])), n_classes=n_classes),
    )


@pytest.fixture
def mx_model(n_hidden: int, input_size: int, X: Array2d):
    import mxnet as mx

    mx_model = mx.gluon.nn.Sequential()
    mx_model.add(
        mx.gluon.nn.Dense(n_hidden),
        mx.gluon.nn.LayerNorm(),
        mx.gluon.nn.Dense(n_hidden, activation="relu"),
        mx.gluon.nn.LayerNorm(),
        mx.gluon.nn.Dense(10, activation="softrelu"),
    )
    mx_model.initialize()
    return mx_model


@pytest.fixture
def model(mx_model) -> Model[Array2d, Array2d]:
    return MXNetWrapper(mx_model)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_roundtrip_conversion():
    import mxnet as mx

    xp_tensor = numpy.zeros((2, 3), dtype="f")
    mx_tensor = xp2mxnet(xp_tensor)
    assert isinstance(mx_tensor, mx.nd.NDArray)
    new_xp_tensor = mxnet2xp(mx_tensor)
    assert numpy.array_equal(xp_tensor, new_xp_tensor)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_gluon_sequential():
    import mxnet as mx

    mx_model = mx.gluon.nn.Sequential()
    mx_model.add(mx.gluon.nn.Dense(12))
    wrapped = MXNetWrapper(mx_model)
    assert isinstance(wrapped, Model)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_built_model(
    model: Model[Array2d, Array2d], X: Array2d, Y: Array1d
):
    # built models are validated more and can perform useful operations:
    assert model.predict(X) is not None
    # They can de/serialized
    assert model.from_bytes(model.to_bytes()) is not None


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_predict(model: Model[Array2d, Array2d], X: Array2d):
    model.predict(X)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_train_overfits(
    model: Model[Array2d, Array2d], X: Array2d, Y: Array1d, answer: int
):
    optimizer = Adam()
    for i in range(100):
        guesses, backprop = model(X, is_train=True)
        d_guesses = (guesses - Y) / guesses.shape[0]
        backprop(d_guesses)
        model.finish_update(optimizer)
    predicted = model.predict(X).argmax()
    assert predicted == answer


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_can_copy_model(model: Model[Array2d, Array2d], X: Array2d):
    model.predict(X)
    copy: Model[Array2d, Array2d] = model.copy()
    assert copy is not None


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_to_bytes(model: Model[Array2d, Array2d], X: Array2d):
    model.predict(X)
    # And can be serialized
    model_bytes = model.to_bytes()
    assert model_bytes is not None
    model.from_bytes(model_bytes)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_to_from_disk(model: Model[Array2d, Array2d], X: Array2d):
    model.predict(X)
    with make_tempdir() as tmp_path:
        model_file = tmp_path / "model.bytes"
        model.to_disk(model_file)
        another_model = model.from_disk(model_file)
        assert another_model is not None


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_from_bytes(model: Model[Array2d, Array2d], X: Array2d):
    model.predict(X)
    model_bytes = model.to_bytes()
    another_model = model.from_bytes(model_bytes)
    assert another_model is not None


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_to_cpu(mx_model, X: Array2d):
    model = MXNetWrapper(mx_model)
    model.predict(X)
    model.to_cpu()


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
@pytest.mark.skipif(not has_cupy_gpu, reason="needs GPU/cupy")
def test_mxnet_wrapper_to_gpu(model: Model[Array2d, Array2d], X: Array2d):
    model.predict(X)
    model.to_gpu(0)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
@pytest.mark.parametrize(
    "data,n_args,kwargs_keys",
    [
        # fmt: off
        (numpy.zeros((2, 3), dtype="f"), 1, []),
        ([numpy.zeros((2, 3), dtype="f"), numpy.zeros((2, 3), dtype="f")], 2, []),
        ((numpy.zeros((2, 3), dtype="f"), numpy.zeros((2, 3), dtype="f")), 2, []),
        ({"a": numpy.zeros((2, 3), dtype="f"), "b": numpy.zeros((2, 3), dtype="f")}, 0, ["a", "b"]),
        (ArgsKwargs((numpy.zeros((2, 3), dtype="f"), numpy.zeros((2, 3), dtype="f")), {"c": numpy.zeros((2, 3), dtype="f")}), 2, ["c"]),
        # fmt: on
    ],
)
def test_mxnet_wrapper_convert_inputs(data, n_args, kwargs_keys):
    import mxnet as mx

    mx_model = mx.gluon.nn.Sequential()
    mx_model.add(mx.gluon.nn.Dense(12))
    mx_model.initialize()
    model = MXNetWrapper(mx_model)
    convert_inputs = model.attrs["convert_inputs"]
    Y, backprop = convert_inputs(model, data, is_train=True)
    check_input_converters(Y, backprop, data, n_args, kwargs_keys, mx.nd.NDArray)


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_thinc_model_subclass(mx_model):
    class CustomModel(Model):
        def fn(self) -> int:
            return 1337

    model = MXNetWrapper(mx_model, model_class=CustomModel)
    assert isinstance(model, CustomModel)
    assert model.fn() == 1337


@pytest.mark.skipif(not has_mxnet, reason="needs MXNet")
def test_mxnet_wrapper_thinc_set_model_name(mx_model):
    model = MXNetWrapper(mx_model, model_name="cool")
    assert model.name == "cool"
