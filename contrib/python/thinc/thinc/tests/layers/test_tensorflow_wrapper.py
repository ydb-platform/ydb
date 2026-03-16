import numpy
import pytest

from thinc.api import (
    Adam,
    ArgsKwargs,
    Linear,
    Model,
    TensorFlowWrapper,
    get_current_ops,
    keras_subclass,
    tensorflow2xp,
    xp2tensorflow,
)
from thinc.compat import has_cupy_gpu, has_tensorflow
from thinc.util import to_categorical

from ..util import check_input_converters, make_tempdir


@pytest.fixture
def n_hidden():
    return 12


@pytest.fixture
def input_size():
    return 784


@pytest.fixture
def n_classes():
    return 10


@pytest.fixture
def answer():
    return 1


@pytest.fixture
def X(input_size):
    ops = get_current_ops()
    return ops.alloc(shape=(1, input_size))


@pytest.fixture
def Y(answer, n_classes):
    ops = get_current_ops()
    return to_categorical(ops.asarray1i([answer]), n_classes=n_classes)


@pytest.fixture
def tf_model(n_hidden, input_size):
    import tensorflow as tf

    tf_model = tf.keras.Sequential(
        [
            tf.keras.layers.Dense(n_hidden, input_shape=(input_size,)),
            tf.keras.layers.LayerNormalization(),
            tf.keras.layers.Dense(n_hidden, activation="relu"),
            tf.keras.layers.LayerNormalization(),
            tf.keras.layers.Dense(10, activation="softmax"),
        ]
    )
    return tf_model


@pytest.fixture
def model(tf_model):
    return TensorFlowWrapper(tf_model)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_roundtrip_conversion():
    import tensorflow as tf

    ops = get_current_ops()
    xp_tensor = ops.alloc2f(2, 3, zeros=True)
    tf_tensor = xp2tensorflow(xp_tensor)
    assert isinstance(tf_tensor, tf.Tensor)
    new_xp_tensor = tensorflow2xp(tf_tensor, ops=ops)
    assert ops.xp.array_equal(xp_tensor, new_xp_tensor)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_construction_requires_keras_model():
    import tensorflow as tf

    keras_model = tf.keras.Sequential([tf.keras.layers.Dense(12, input_shape=(12,))])
    assert isinstance(TensorFlowWrapper(keras_model), Model)
    with pytest.raises(ValueError):
        TensorFlowWrapper(Linear(2, 3))


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_built_model(model, X, Y):
    # built models are validated more and can perform useful operations:
    assert model.predict(X) is not None
    # Can print a keras summary
    assert str(model.shims[0]) != ""
    # They can de/serialized
    assert model.from_bytes(model.to_bytes()) is not None


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_predict(model, X):
    model.predict(X)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_train_overfits(model, X, Y, answer):
    optimizer = Adam()
    ops = get_current_ops()
    for i in range(100):
        guesses, backprop = model(X, is_train=True)
        # Ensure that the tensor is type-compatible with the current backend.
        guesses = ops.asarray(guesses)

        d_guesses = (guesses - Y) / guesses.shape[0]
        backprop(d_guesses)
        model.finish_update(optimizer)
    predicted = model.predict(X).argmax()
    assert predicted == answer


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_accumulate_gradients(model, X, Y, answer):
    import tensorflow as tf

    optimizer = Adam()
    gradients = []
    ops = get_current_ops()
    for i in range(3):
        guesses, backprop = model(X, is_train=True)
        # Ensure that the tensor is type-compatible with the current backend.
        guesses = ops.asarray(guesses)

        d_guesses = (guesses - Y) / guesses.shape[0]
        backprop(d_guesses)
        shim_grads = [tf.identity(var) for var in model.shims[0].gradients]
        gradients.append(shim_grads)

    # Apply the gradients
    model.finish_update(optimizer)
    assert model.shims[0].gradients is None

    # Compare prev/next pairs and ensure their gradients have changed
    for i in range(len(gradients)):
        # Skip the first one
        if i == 0:
            continue
        found_diff = False
        curr_grads = gradients[i]
        prev_grads = gradients[i - 1]
        for curr, prev in zip(curr_grads, prev_grads):
            if (prev != curr).numpy().any():
                found_diff = True
        assert found_diff is True


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_serialize_model_subclass(
    X, Y, input_size, n_classes, answer
):
    import tensorflow as tf

    input_shape = (1, input_size)
    ops = get_current_ops()

    @keras_subclass(
        "foo.v1",
        X=ops.alloc2f(*input_shape),
        Y=to_categorical(ops.asarray1i([1]), n_classes=n_classes),
        input_shape=input_shape,
    )
    class CustomKerasModel(tf.keras.Model):
        def __init__(self, **kwargs):
            super(CustomKerasModel, self).__init__(**kwargs)
            self.in_dense = tf.keras.layers.Dense(
                12, name="in_dense", input_shape=input_shape
            )
            self.out_dense = tf.keras.layers.Dense(
                n_classes, name="out_dense", activation="softmax"
            )

        def call(self, inputs) -> tf.Tensor:
            x = self.in_dense(inputs)
            return self.out_dense(x)

    model = TensorFlowWrapper(CustomKerasModel())
    # Train the model to predict the right single answer
    optimizer = Adam()
    for i in range(50):
        guesses, backprop = model(X, is_train=True)
        # Ensure that the tensor is type-compatible with the current backend.
        guesses = ops.asarray(guesses)

        d_guesses = (guesses - Y) / guesses.shape[0]
        backprop(d_guesses)
        model.finish_update(optimizer)
    predicted = model.predict(X).argmax()
    assert predicted == answer

    # Save then Load the model from bytes
    model.from_bytes(model.to_bytes())

    # The from_bytes model gets the same answer
    assert model.predict(X).argmax() == answer


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_keras_subclass_decorator_compile_args():
    import tensorflow as tf

    class UndecoratedModel(tf.keras.Model):
        def call(self, inputs):
            return inputs

    # Can't wrap an undecorated keras subclass model
    with pytest.raises(ValueError):
        TensorFlowWrapper(UndecoratedModel())

    @keras_subclass(
        "TestModel",
        X=numpy.array([0.0, 0.0]),
        Y=numpy.array([0.5]),
        input_shape=(2,),
        compile_args={"loss": "binary_crossentropy"},
    )
    class TestModel(tf.keras.Model):
        def call(self, inputs):
            return inputs

    model = TensorFlowWrapper(TestModel())
    model = model.from_bytes(model.to_bytes())

    assert model.shims[0]._model.loss == "binary_crossentropy"
    assert isinstance(model, Model)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_keras_subclass_decorator():
    import tensorflow as tf

    class UndecoratedModel(tf.keras.Model):
        def call(self, inputs):
            return inputs

    # Can't wrap an undecorated keras subclass model
    with pytest.raises(ValueError):
        TensorFlowWrapper(UndecoratedModel())

    @keras_subclass(
        "TestModel", X=numpy.array([0.0, 0.0]), Y=numpy.array([0.5]), input_shape=(2,)
    )
    class TestModel(tf.keras.Model):
        def call(self, inputs):
            return inputs

    # Can wrap an decorated keras subclass model
    assert isinstance(TensorFlowWrapper(TestModel()), Model)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_keras_subclass_decorator_capture_args_kwargs(
    X, Y, input_size, n_classes, answer
):
    import tensorflow as tf

    @keras_subclass(
        "TestModel", X=numpy.array([0.0, 0.0]), Y=numpy.array([0.5]), input_shape=(2,)
    )
    class TestModel(tf.keras.Model):
        def __init__(self, custom=False, **kwargs):
            super().__init__(self)
            # This is to force the mode to pass the captured arguments
            # or fail.
            assert custom is True
            assert kwargs.get("other", None) is not None

        def call(self, inputs):
            return inputs

    # Can wrap an decorated keras subclass model
    model = TensorFlowWrapper(TestModel(True, other=1337))

    assert hasattr(model.shims[0]._model, "eg_args")
    args_kwargs = model.shims[0]._model.eg_args
    assert True in args_kwargs.args
    assert "other" in args_kwargs.kwargs

    # Raises an error if the args/kwargs is not serializable
    obj = {}
    obj["key"] = obj
    with pytest.raises(ValueError):
        TensorFlowWrapper(TestModel(True, other=obj))

    # Provides the same arguments when copying a capture model
    model = model.from_bytes(model.to_bytes())


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_can_copy_model(model):
    copy = model.copy()
    assert copy is not None


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_print_summary(model, X):
    summary = str(model.shims[0])
    # Summary includes the layers of our model
    assert "layer_normalization" in summary
    assert "dense" in summary
    # And counts of params
    assert "Total params" in summary
    assert "Trainable params" in summary
    assert "Non-trainable params" in summary


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_to_bytes(model, X):
    # And can be serialized
    model_bytes = model.to_bytes()
    assert model_bytes is not None
    model.from_bytes(model_bytes)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_to_from_disk(model, X, Y, answer):
    with make_tempdir() as tmp_path:
        model_file = tmp_path / "model.h5"
        model.to_disk(model_file)
        another_model = model.from_disk(model_file)
        assert another_model is not None


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_from_bytes(model, X):
    model.predict(X)
    model_bytes = model.to_bytes()
    another_model = model.from_bytes(model_bytes)
    assert another_model is not None


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_use_params(model, X, Y, answer):
    optimizer = Adam()
    ops = get_current_ops()
    with model.use_params(optimizer.averages):
        assert model.predict(X).argmax() is not None
    for i in range(10):
        guesses, backprop = model.begin_update(X)
        # Ensure that the tensor is type-compatible with the current backend.
        guesses = ops.asarray(guesses)

        d_guesses = (guesses - Y) / guesses.shape[0]
        backprop(d_guesses)
        model.finish_update(optimizer)
    with model.use_params(optimizer.averages):
        predicted = model.predict(X).argmax()
    assert predicted == answer


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_to_cpu(tf_model):
    model = TensorFlowWrapper(tf_model)
    model.to_cpu()


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
@pytest.mark.skipif(not has_cupy_gpu, reason="needs GPU/cupy")
def test_tensorflow_wrapper_to_gpu(model, X):
    model.to_gpu(0)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
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
def test_tensorflow_wrapper_convert_inputs(data, n_args, kwargs_keys):
    import tensorflow as tf

    keras_model = tf.keras.Sequential([tf.keras.layers.Dense(12, input_shape=(12,))])
    model = TensorFlowWrapper(keras_model)
    convert_inputs = model.attrs["convert_inputs"]
    Y, backprop = convert_inputs(model, data, is_train=True)
    check_input_converters(Y, backprop, data, n_args, kwargs_keys, tf.Tensor)


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_thinc_model_subclass(tf_model):
    class CustomModel(Model):
        def fn(self):
            return 1337

    model = TensorFlowWrapper(tf_model, model_class=CustomModel)
    assert isinstance(model, CustomModel)
    assert model.fn() == 1337


@pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow")
def test_tensorflow_wrapper_thinc_set_model_name(tf_model):
    model = TensorFlowWrapper(tf_model, model_name="cool")
    assert model.name == "cool"
