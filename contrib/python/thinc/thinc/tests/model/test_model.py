import threading
import time
from collections import Counter

import numpy
import pytest

from thinc.api import (
    Adam,
    CupyOps,
    Dropout,
    Linear,
    Model,
    Relu,
    Shim,
    Softmax,
    chain,
    change_attr_values,
    concatenate,
    set_dropout_rate,
    use_ops,
    with_debug,
    wrap_model_recursive,
)
from thinc.compat import has_cupy_gpu

from ..util import make_tempdir


@pytest.fixture
def model_with_no_args():
    return Linear()


def create_model(name):
    return Model(name, lambda X: (X, lambda dY: dY))


def test_model_defaults_to_cpu(model_with_no_args):
    assert not isinstance(model_with_no_args.ops, CupyOps)


def test_models_get_different_ids(model_with_no_args):
    model1 = Linear()
    model2 = Linear()
    assert model1.id != model2.id


def test_model_init():
    class MyShim(Shim):
        name = "testshim"

    model_a = create_model("a")
    model = Model(
        "test",
        lambda X: (X, lambda dY: dY),
        dims={"nI": 10, "nO": None},
        params={"W": numpy.zeros((10,)), "b": None},
        refs={"a": model_a, "b": None},
        attrs={"foo": "bar"},
        shims=[MyShim(None)],
        layers=[model_a, model_a],
    )
    assert model.has_param("W")
    assert model.get_param("W").shape == (10,)
    assert model.has_param("b") is None
    with pytest.raises(KeyError):
        model.get_param("b")
    with pytest.raises(KeyError):
        model.get_param("X")
    model.set_param("X", numpy.zeros((10,)))
    assert model.has_param("X")
    assert model.get_param("X").shape == (10,)
    with model.use_params({(model.id, "X"): numpy.ones((10,))}):
        assert numpy.array_equal(model.get_param("X"), numpy.ones((10,)))
    assert numpy.array_equal(model.get_param("X"), numpy.zeros((10,)))
    assert not model.has_grad("W")
    assert not model.has_grad("xyz")
    with pytest.raises(KeyError):
        model.get_grad("b")
    model.set_param("W", model.ops.alloc1f(10))
    model.set_grad("W", model.ops.alloc1f(10))
    with pytest.raises(ValueError):
        model.inc_grad("W", numpy.zeros((5, 0)))
    assert model.has_dim("nI")
    assert model.get_dim("nI") == 10
    with pytest.raises(KeyError):
        model.get_dim("xyz")
    with pytest.raises(ValueError):
        model.get_dim("nO")
    assert model.has_ref("a")
    assert model.get_ref("a").name == "a"
    assert not model.has_ref("xyz")
    with pytest.raises(KeyError):
        model.get_ref("xyz")
    assert model.has_ref("b") is None
    with pytest.raises(ValueError):
        model.get_ref("b")
    model.set_ref("c", model_a)
    assert model.has_ref("c")
    assert model.get_ref("c").name == "a"
    with pytest.raises(ValueError):
        model.set_ref("c", create_model("c"))
    assert "foo" in model.attrs
    assert "bar" not in model.attrs
    assert model.attrs["foo"] == "bar"
    with pytest.raises(KeyError):
        model.attrs["bar"]
    model.attrs["bar"] = "baz"
    model_copy = model.copy()
    assert model_copy.name == "test"


def test_model_set_dim():
    class MyShim(Shim):
        name = "testshim"

    model_a = create_model("a")
    model = Model(
        "test",
        lambda X: (X, lambda dY: dY),
        dims={"nI": 5, "nO": None},
        params={"W": None, "b": None},
        refs={"a": model_a, "b": None},
        attrs={"foo": "bar"},
        shims=[MyShim(None)],
        layers=[model_a, model_a],
    )
    with pytest.raises(ValueError):
        model.set_dim("nI", 10)
    # force can be used before any parameters are set
    model.set_dim("nI", 10, force=True)
    model.set_param("W", model.ops.alloc1f(10))
    model.set_grad("W", model.ops.alloc1f(10))
    assert model.has_dim("nI")
    assert model.get_dim("nI") == 10
    with pytest.raises(KeyError):
        model.set_dim("xyz", 20)
    with pytest.raises(ValueError):
        model.set_dim("nI", 20)
    # force can't be used after any parameter is set
    with pytest.raises(ValueError):
        model.set_dim("nI", 20, force=True)


def test_param_names():
    model = create_model("tmp")
    assert model.param_names == tuple()
    model.set_param("param1", None)
    assert model.param_names == ("param1",)
    model.set_param("param2", None)
    assert model.param_names == ("param1", "param2")


def test_grad_names():
    model = create_model("tmp")
    assert model.grad_names == tuple()
    model.set_param("param1", model.ops.alloc2f(4, 4))
    model.set_grad("param1", model.ops.alloc2f(4, 4) + 1)
    assert model.grad_names == ("param1",)


def test_dim_names():
    model = Linear(5, 3)
    assert model.dim_names == ("nO", "nI")


def test_model_set_reference():
    parent = create_model("parent")
    child = create_model("child")
    grandchild = create_model("child")
    parent.layers.append(child)
    assert parent.ref_names == tuple()
    parent.set_ref("kid", child)
    assert parent.ref_names == ("kid",)
    assert parent.get_ref("kid") is child
    child.layers.append(grandchild)
    with pytest.raises(KeyError):
        parent.get_ref("grandkid")
    parent.set_ref("grandkid", grandchild)
    assert parent.get_ref("grandkid") is grandchild
    parent.remove_node(grandchild)
    assert grandchild not in child.layers
    assert not parent.has_ref("grandkind")


def test_maybe_methods():
    model = Linear(5)
    assert model.maybe_get_dim("nI") is None
    model.set_dim("nI", 4)
    assert model.maybe_get_dim("nI") == 4
    assert model.maybe_get_ref("boo") is None
    assert model.maybe_get_param("W") is None
    model.initialize()
    assert model.maybe_get_param("W") is not None


def test_model_can_save_to_disk(model_with_no_args):
    with make_tempdir() as path:
        model_with_no_args.to_disk(path / "thinc_model")


def test_model_can_load_from_disk(model_with_no_args):
    with make_tempdir() as path:
        model_with_no_args.to_disk(path / "thinc_model")
        m2 = model_with_no_args.from_disk(path / "thinc_model")
    assert model_with_no_args.to_bytes() == m2.to_bytes()


def test_model_can_roundtrip_with_path_subclass(model_with_no_args, pathy_fixture):
    path = pathy_fixture / "thinc_model"
    model_with_no_args.to_disk(path)
    m2 = model_with_no_args.from_disk(path)
    assert model_with_no_args.to_bytes() == m2.to_bytes()


def test_change_attr_values(model_with_no_args):
    model = model_with_no_args
    model.name = "target"
    model.attrs["has_var"] = False
    change_attr_values(model, {"target": {"has_var": True, "error": True}})
    assert model.attrs["has_var"] is True
    assert "error" not in model.attrs


def test_set_dropout():
    model = Dropout()
    assert model.attrs["dropout_rate"] == 0.0
    set_dropout_rate(model, 0.2)
    assert model.attrs["dropout_rate"] == 0.2


def test_set_dropout_2(model_with_no_args):
    model = model_with_no_args
    model.name = "dropout"
    model.attrs["dropout_rate"] = 0.0
    set_dropout_rate(model, 0.2)
    assert model.attrs["dropout_rate"] == 0.2


def test_bind_plus():
    with Model.define_operators({"+": lambda a, b: (a.name, b.name)}):
        m = create_model(name="a") + create_model(name="b")
        assert m == ("a", "b")


def test_plus_chain():
    with Model.define_operators({"+": lambda a, b: a}):
        m = (
            create_model(name="a")
            + create_model(name="b")
            + create_model(name="c")
            + create_model(name="d")
        )
        assert m.name == "a"


def test_overload_operators_in_subthread():
    """Test we can create a model in a child thread with overloaded operators."""
    # Worker1 will start and run, while worker 2 sleeps after Model.define_operators.
    # Without thread-safety, worker2 will find that its operator definitions
    # have been removed, causing an error.
    worker1 = threading.Thread(target=_overload_plus, args=("+", 0))
    worker2 = threading.Thread(target=_overload_plus, args=("*", 1))
    worker2.start()
    worker1.start()
    worker1.join()
    worker2.join()

    worker1 = threading.Thread(target=_overload_plus, args=("+", 1))
    worker2 = threading.Thread(target=_overload_plus, args=("*", 0))
    worker2.start()
    worker1.start()
    worker1.join()
    worker2.join()


def _overload_plus(operator, sleep):
    m1 = create_model(name="a")
    m2 = create_model(name="b")
    with Model.define_operators({operator: lambda a, b: a.name + b.name}):
        time.sleep(sleep)
        if operator == "+":
            value = m1 + m2
        else:
            value = m1 * m2
    assert value == "ab"
    assert Model._context_operators.get() == {}


def test_nested_operator_contexts():
    m1 = create_model(name="a")
    m2 = create_model(name="b")
    assert Model._context_operators.get() == {}
    with Model.define_operators({"+": lambda a, b: a.name + b.name}):
        value = m1 + m2
        with pytest.raises(TypeError):
            value = m1 * m2
        with Model.define_operators({"*": lambda a, b: a.name + b.name}):
            with pytest.raises(TypeError):
                value = m1 + m2
            value = m1 * m2
            with Model.define_operators({"-": lambda a, b: a.name + b.name}):
                with pytest.raises(TypeError):
                    value = m1 + m2
                value = m1 - m2
            with pytest.raises(TypeError):
                value = m1 + m2
            value = m1 * m2
        value = m1 + m2
        with pytest.raises(TypeError):
            value = m1 * m2
    assert value == "ab"
    assert Model._context_operators.get() == {}


@pytest.mark.parametrize("op", "+ - * @ / // % ** << >> & ^ |".split())
def test_all_operators(op):
    m1 = Linear()
    m2 = Linear()
    with Model.define_operators({op: lambda a, b: a.name + b.name}):
        if op == "+":
            value = m1 + m2
        else:
            with pytest.raises(TypeError):
                value = m1 + m2
        if op == "-":
            value = m1 - m2
        else:
            with pytest.raises(TypeError):
                value = m1 - m2

        if op == "*":
            value = m1 * m2
        else:
            with pytest.raises(TypeError):
                value = m1 * m2

        if op == "@":
            value = m1.__matmul__(m2)  # Be kind to Python 2...
        else:
            with pytest.raises(TypeError):
                value = m1.__matmul__(m2)

        if op == "/":
            value = m1 / m2
        else:
            with pytest.raises(TypeError):
                value = m1 / m2

        if op == "//":
            value = m1 // m2
        else:
            with pytest.raises(TypeError):
                value = m1 // m2
        if op == "^":
            value = m1 ^ m2
        else:
            with pytest.raises(TypeError):
                value = m1 ^ m2
        if op == "%":
            value = m1 % m2
        else:
            with pytest.raises(TypeError):
                value = m1 % m2
        if op == "**":
            value = m1**m2
        else:
            with pytest.raises(TypeError):
                value = m1**m2
        if op == "<<":
            value = m1 << m2
        else:
            with pytest.raises(TypeError):
                value = m1 << m2
        if op == ">>":
            value = m1 >> m2
        else:
            with pytest.raises(TypeError):
                value = m1 >> m2
        if op == "&":
            value = m1 & m2
        else:
            with pytest.raises(TypeError):
                value = m1 & m2
        if op == "^":
            value = m1 ^ m2
        else:
            with pytest.raises(TypeError):
                value = m1 ^ m2
        if op == "|":
            value = m1 | m2
        else:
            with pytest.raises(TypeError):
                value = m1 | m2  # noqa: F841
    assert Model._context_operators.get() == {}


def test_unique_id_multithreading():
    """Create a bunch of threads and assert they all get unique IDs"""

    list_of_ids = []

    def get_model_id(id_list, index):
        id_list.append(create_model(name=f"worker{index}").id)

    counter = 0
    while len(list_of_ids) < 1000:
        workers = []
        for i in range(50):
            w = threading.Thread(target=get_model_id, args=(list_of_ids, counter))
            workers.append(w)
            counter += 1
        for w in workers:
            w.start()
        for w in workers:
            w.join()

    assert len(list_of_ids) == len(list(set(list_of_ids)))


@pytest.mark.skipif(not has_cupy_gpu, reason="needs CuPy GPU")
def test_model_gpu():
    pytest.importorskip("ml_datasets")
    import ml_datasets

    with use_ops("cupy"):
        n_hidden = 32
        dropout = 0.2
        (train_X, train_Y), (dev_X, dev_Y) = ml_datasets.mnist()
        model = chain(
            Relu(nO=n_hidden, dropout=dropout),
            Relu(nO=n_hidden, dropout=dropout),
            Softmax(),
        )
        # make sure the data is on the right device
        train_X = model.ops.asarray(train_X)
        train_Y = model.ops.asarray(train_Y)
        dev_X = model.ops.asarray(dev_X)
        dev_Y = model.ops.asarray(dev_Y)

        model.initialize(X=train_X[:5], Y=train_Y[:5])
        optimizer = Adam(0.001)
        batch_size = 128

        for i in range(2):
            batches = model.ops.multibatch(batch_size, train_X, train_Y, shuffle=True)
            for X, Y in batches:
                Yh, backprop = model.begin_update(X)
                backprop(Yh - Y)
                model.finish_update(optimizer)
            # Evaluate and print progress
            correct = 0
            total = 0
            for X, Y in model.ops.multibatch(batch_size, dev_X, dev_Y):
                Yh = model.predict(X)
                correct += (Yh.argmax(axis=1) == Y.argmax(axis=1)).sum()
                total += Yh.shape[0]


def test_replace_node():
    relu1 = Relu(5)
    relu2 = Relu(5)
    relu_chain = chain(relu1, relu2)
    relu1_debug = with_debug(relu1)
    debug = Model(
        "test",
        lambda X: (X, lambda dY: dY),
        layers=[relu1, relu2, relu1, relu_chain],
        refs={"relu1": relu1, "relu2": relu2, "relu3": relu1},
    )
    debug.replace_node(relu1, relu1_debug)
    assert debug.layers[0] == relu1_debug
    assert debug.layers[1] == relu2
    assert debug.layers[2] == relu1_debug
    assert debug.get_ref("relu1") == relu1_debug
    assert debug.get_ref("relu2") == relu2
    assert debug.get_ref("relu3") == relu1_debug

    # Check that nodes are replaced recursively
    assert debug.layers[3] == relu_chain
    assert debug.layers[3].layers[0] == relu1_debug
    assert debug.layers[3].layers[1] == relu2


def test_replace_node_with_indirect_node_ref():
    #  a
    # / \
    # x  b[y=y]
    # |  |
    # y  x
    #    |
    #    y

    def dummy_model(name, layers):
        return Model(name, lambda model, X, is_train: ..., layers=layers)

    y = dummy_model("y", [])
    x = dummy_model("x", [y])

    y_debug = with_debug(y)

    b = dummy_model("b", [x])
    b.set_ref("y", y)

    a = chain(x, b)
    a.name = "a"

    a.replace_node(y, y_debug)

    assert a.layers[0].layers[0] == y_debug
    assert a.layers[1].layers[0].layers[0] == y_debug
    assert a.layers[1].get_ref("y") == y_debug


def test_with_debug():
    pytest.importorskip("ml_datasets")
    import ml_datasets

    (train_X, train_Y), (dev_X, dev_Y) = ml_datasets.mnist()

    counts = Counter()

    def on_init(*_):
        counts["init"] += 1

    def on_forward(*_):
        counts["forward"] += 1

    def on_backprop(*_):
        counts["backprop"] += 1

    relu = Relu()
    relu2 = with_debug(
        Relu(), on_init=on_init, on_forward=on_forward, on_backprop=on_backprop
    )
    chained = chain(relu, relu2, relu2)
    chained.initialize(X=train_X[:5], Y=train_Y[:5])
    _, backprop = chained(X=train_X[:5], is_train=False)

    # Not real loss gradients, but we don't care for testing.
    backprop(train_Y[:5])

    # Four times forward, because initialization also applies forward for
    # validation.
    assert counts == {"init": 2, "forward": 4, "backprop": 2}


def test_recursive_wrap():
    def dummy_model(name, layers):
        return Model(name, lambda model, X, is_train: ..., layers=layers)

    # Check:
    #
    # * Recursion: chain -> relu
    # * Multiple sublayers: chain -> [relu, relu]

    relu = Relu(5)
    chained = chain(relu, relu)
    chained_debug = wrap_model_recursive(
        chained, lambda model: dummy_model(f"dummy({model.name})", [model])
    )

    assert chained_debug.name == "dummy(relu>>relu)"
    assert chained_debug.layers[0] is chained
    assert chained_debug.layers[0].layers[0].name == "dummy(relu)"
    assert chained_debug.layers[0].layers[0].layers[0] is relu
    assert chained_debug.layers[0].layers[1].name == "dummy(relu)"
    assert chained_debug.layers[0].layers[1].layers[0] is relu


def test_recursive_double_wrap():
    def dummy_model(name, layers):
        return Model(name, lambda model, X, is_train: ..., layers=layers)

    relu = Relu(5)
    chained = chain(relu, relu)
    concat = concatenate(chained, chained, relu)
    concat_wrapped = wrap_model_recursive(
        concat, lambda model: dummy_model(f"dummy({model.name})", [model])
    )

    n_debug = 0
    for model in concat_wrapped.walk():
        if model.name.startswith("dummy"):
            n_debug += 1

    # There should be 3 unique dummy wrappers:
    # * Around concatenate.
    # * Around chain.
    # * Around relu.
    assert n_debug == 3

    assert concat_wrapped.layers[0].layers[0].layers[0].layers[0].name == "dummy(relu)"
    assert concat_wrapped.layers[0].layers[0].layers[0].layers[1].name == "dummy(relu)"
    assert concat_wrapped.layers[0].layers[1].layers[0].layers[0].name == "dummy(relu)"
    assert concat_wrapped.layers[0].layers[1].layers[0].layers[1].name == "dummy(relu)"
    assert concat_wrapped.layers[0].layers[2].name == "dummy(relu)"


def test_wrap_non_child_references():
    relu = Relu(5)
    relu2 = Relu(5)
    chained = chain(relu, relu)
    chained2 = chain(relu2, chained)
    chained2.set_ref("relu", relu)
    # Fails in case non-child references cannot be set.
    wrap_model_recursive(chained2, with_debug)


def test_walk_dfs():
    relu = Relu(5)
    relu2 = Relu(5)
    inner_chain = chain(relu, relu2)
    chained = chain(inner_chain, inner_chain)
    assert list(chained.walk(order="dfs_pre")) == [chained, inner_chain, relu, relu2]
    assert list(chained.walk(order="dfs_post")) == [
        relu,
        relu2,
        inner_chain,
        chained,
    ]


def test_walk_bfs_post_order_fails():
    relu = Relu(5)
    with pytest.raises(ValueError, match="Invalid order"):
        relu.walk(order="dfs_post_order")


def test_model_copy_with_loop():
    class MyShim(Shim):
        name = "testshim"

        def to_bytes(self):
            return test_replace_node_with_indirect_node_ref

        def from_bytes(self, bytes):
            pass

    model_a = create_model("a")
    working_shim = MyShim(None)
    layer = Model(
        "test",
        lambda X: (X, lambda dY: dY),
        dims={"nI": 5, "nO": 5},
        params={"W": numpy.zeros((10,)), "b": None},
        refs={"a": model_a, "b": None},
        attrs={"foo": "bar"},
        shims=[working_shim],
        layers=[model_a, model_a],
    )
    layer2 = Model(
        "test2",
        lambda X: (X, lambda dY: dY),
        dims={"nI": 5, "nO": 5},
        params={"W": numpy.zeros((10,)), "b": None},
        refs={"a": model_a, "b": None},
        attrs={"foo": "bar"},
        shims=[working_shim],
        layers=[model_a, model_a],
    )
    relu = Relu(5)
    model = chain(layer, relu, layer, layer2)
    model2 = model.copy()
    model.from_dict(model2.to_dict())
    assert model2.name == "test>>relu>>test>>test2"
    assert model2.layers[0] == model2.layers[2]
    assert id(model2.layers[0].shims[0]) == id(model2.layers[3].shims[0])
