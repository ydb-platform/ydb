import pytest
import srsly

from thinc.api import (
    Linear,
    Maxout,
    Model,
    Shim,
    chain,
    deserialize_attr,
    serialize_attr,
    with_array,
)


@pytest.fixture
def linear():
    return Linear(5, 3)


class SerializableAttr:
    value = "foo"

    def to_bytes(self):
        return self.value.encode("utf8")

    def from_bytes(self, data):
        self.value = f"{data.decode('utf8')} from bytes"
        return self


class SerializableShim(Shim):
    name = "testshim"
    value = "shimdata"

    def to_bytes(self):
        return self.value.encode("utf8")

    def from_bytes(self, data):
        self.value = f"{data.decode('utf8')} from bytes"
        return self


def test_pickle_with_flatten(linear):
    Xs = [linear.ops.alloc2f(2, 3), linear.ops.alloc2f(4, 3)]
    model = with_array(linear).initialize()
    pickled = srsly.pickle_dumps(model)
    loaded = srsly.pickle_loads(pickled)
    Ys = loaded.predict(Xs)
    assert len(Ys) == 2
    assert Ys[0].shape == (Xs[0].shape[0], linear.get_dim("nO"))
    assert Ys[1].shape == (Xs[1].shape[0], linear.get_dim("nO"))


def test_simple_model_roundtrip_bytes():
    model = Maxout(5, 10, nP=2).initialize()
    b = model.get_param("b")
    b += 1
    data = model.to_bytes()
    b = model.get_param("b")
    b -= 1
    model = model.from_bytes(data)
    assert model.get_param("b")[0, 0] == 1


def test_simple_model_roundtrip_bytes_length():
    """Ensure that serialization of non-initialized weight matrices goes fine"""
    model1 = Maxout(5, 10, nP=2)
    model2 = Maxout(5, 10, nP=2)

    data1 = model1.to_bytes()
    model2 = model2.from_bytes(data1)
    data2 = model2.to_bytes()

    assert data1 == data2
    assert len(data1) == len(data2)


def test_simple_model_roundtrip_bytes_serializable_attrs():
    fwd = lambda model, X, is_train: (X, lambda dY: dY)
    attr = SerializableAttr()
    assert attr.value == "foo"
    assert attr.to_bytes() == b"foo"
    model = Model("test", fwd, attrs={"test": attr})
    model.initialize()

    @serialize_attr.register(SerializableAttr)
    def serialize_attr_custom(_, value, name, model):
        return value.to_bytes()

    @deserialize_attr.register(SerializableAttr)
    def deserialize_attr_custom(_, value, name, model):
        return SerializableAttr().from_bytes(value)

    model_bytes = model.to_bytes()
    model = model.from_bytes(model_bytes)
    assert "test" in model.attrs
    assert model.attrs["test"].value == "foo from bytes"


def test_multi_model_roundtrip_bytes():
    model = chain(Maxout(5, 10, nP=2), Maxout(2, 3)).initialize()
    b = model.layers[0].get_param("b")
    b += 1
    b = model.layers[1].get_param("b")
    b += 2
    data = model.to_bytes()
    b = model.layers[0].get_param("b")
    b -= 1
    b = model.layers[1].get_param("b")
    b -= 2
    model = model.from_bytes(data)
    assert model.layers[0].get_param("b")[0, 0] == 1
    assert model.layers[1].get_param("b")[0, 0] == 2


def test_multi_model_load_missing_dims():
    model = chain(Maxout(5, 10, nP=2), Maxout(2, 3)).initialize()
    b = model.layers[0].get_param("b")
    b += 1
    b = model.layers[1].get_param("b")
    b += 2
    data = model.to_bytes()

    model2 = chain(Maxout(5, nP=None), Maxout(nP=None))
    model2 = model2.from_bytes(data)
    assert model2.layers[0].get_param("b")[0, 0] == 1
    assert model2.layers[1].get_param("b")[0, 0] == 2


def test_serialize_model_shims_roundtrip_bytes():
    fwd = lambda model, X, is_train: (X, lambda dY: dY)
    test_shim = SerializableShim(None)
    shim_model = Model("shimmodel", fwd, shims=[test_shim])
    model = chain(Linear(2, 3), shim_model, Maxout(2, 3))
    model.initialize()
    assert model.layers[1].shims[0].value == "shimdata"
    model_bytes = model.to_bytes()
    with pytest.raises(ValueError):
        Linear(2, 3).from_bytes(model_bytes)
    test_shim = SerializableShim(None)
    shim_model = Model("shimmodel", fwd, shims=[test_shim])
    new_model = chain(Linear(2, 3), shim_model, Maxout(2, 3)).from_bytes(model_bytes)
    assert new_model.layers[1].shims[0].value == "shimdata from bytes"


def test_serialize_refs_roundtrip_bytes():
    fwd = lambda model, X, is_train: (X, lambda dY: dY)
    model_a = Model("a", fwd)
    model = Model("test", fwd, refs={"a": model_a, "b": None}).initialize()
    with pytest.raises(ValueError):  # ref not in nodes
        model.to_bytes()
    model = Model("test", fwd, refs={"a": model_a, "b": None}, layers=[model_a])
    assert model.ref_names == ("a", "b")
    model_bytes = model.to_bytes()
    with pytest.raises(ValueError):
        Model("test", fwd).from_bytes(model_bytes)
    new_model = Model("test", fwd, layers=[model_a])
    new_model.from_bytes(model_bytes)
    assert new_model.ref_names == ("a", "b")


def test_serialize_attrs():
    fwd = lambda model, X, is_train: (X, lambda dY: dY)
    attrs = {"test": "foo"}
    model1 = Model("test", fwd, attrs=attrs).initialize()
    bytes_attr = serialize_attr(model1.attrs["test"], attrs["test"], "test", model1)
    assert bytes_attr == srsly.msgpack_dumps("foo")
    model2 = Model("test", fwd, attrs={"test": ""})
    result = deserialize_attr(model2.attrs["test"], bytes_attr, "test", model2)
    assert result == "foo"

    # Test objects with custom serialization functions
    @serialize_attr.register(SerializableAttr)
    def serialize_attr_custom(_, value, name, model):
        return value.to_bytes()

    @deserialize_attr.register(SerializableAttr)
    def deserialize_attr_custom(_, value, name, model):
        return SerializableAttr().from_bytes(value)

    attrs = {"test": SerializableAttr()}
    model3 = Model("test", fwd, attrs=attrs)
    bytes_attr = serialize_attr(model3.attrs["test"], attrs["test"], "test", model3)
    assert bytes_attr == b"foo"
    model4 = Model("test", fwd, attrs=attrs)
    assert model4.attrs["test"].value == "foo"
    result = deserialize_attr(model4.attrs["test"], bytes_attr, "test", model4)
    assert result.value == "foo from bytes"


def test_simple_model_can_from_dict():
    model = Maxout(5, 10, nP=2).initialize()
    model_dict = model.to_dict()
    assert model.can_from_dict(model_dict)
    # Test check without initialize
    assert Maxout(5, 10, nP=2).can_from_dict(model_dict)
    # Test not-strict check
    assert not Maxout(10, 5, nP=2).can_from_dict(model_dict)
    assert Maxout(5, nP=2).can_from_dict(model_dict)


def test_multi_model_can_from_dict():
    model = chain(Maxout(5, 10, nP=2), Maxout(2, 3)).initialize()
    model_dict = model.to_dict()
    assert model.can_from_dict(model_dict)
    assert chain(Maxout(5, 10, nP=2), Maxout(2, 3)).can_from_dict(model_dict)
    resized = chain(Maxout(5, 10, nP=3), Maxout(2, 3))
    assert not resized.can_from_dict(model_dict)
