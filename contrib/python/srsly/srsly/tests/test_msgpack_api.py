import pytest
from pathlib import Path
import datetime
from mock import patch
import numpy

from srsly._msgpack_api import read_msgpack, write_msgpack
from srsly._msgpack_api import msgpack_loads, msgpack_dumps
from srsly._msgpack_api import msgpack_encoders, msgpack_decoders
from .util import make_tempdir


def test_msgpack_dumps():
    data = {"hello": "world", "test": 123}
    expected = [b"\x82\xa5hello\xa5world\xa4test{", b"\x82\xa4test{\xa5hello\xa5world"]
    msg = msgpack_dumps(data)
    assert msg in expected


def test_msgpack_loads():
    msg = b"\x82\xa5hello\xa5world\xa4test{"
    data = msgpack_loads(msg)
    assert len(data) == 2
    assert data["hello"] == "world"
    assert data["test"] == 123


def test_read_msgpack_file():
    file_contents = b"\x81\xa5hello\xa5world"
    with make_tempdir({"tmp.msg": file_contents}, mode="wb") as temp_dir:
        file_path = temp_dir / "tmp.msg"
        assert file_path.exists()
        data = read_msgpack(file_path)
    assert len(data) == 1
    assert data["hello"] == "world"


def test_read_msgpack_file_invalid():
    file_contents = b"\xa5hello\xa5world"
    with make_tempdir({"tmp.msg": file_contents}, mode="wb") as temp_dir:
        file_path = temp_dir / "tmp.msg"
        assert file_path.exists()
        with pytest.raises(ValueError):
            read_msgpack(file_path)


def test_write_msgpack_file():
    data = {"hello": "world", "test": 123}
    expected = [b"\x82\xa5hello\xa5world\xa4test{", b"\x82\xa4test{\xa5hello\xa5world"]
    with make_tempdir(mode="wb") as temp_dir:
        file_path = temp_dir / "tmp.msg"
        write_msgpack(file_path, data)
        with Path(file_path).open("rb") as f:
            assert f.read() in expected


@patch("srsly.msgpack._msgpack_numpy.np", None)
@patch("srsly.msgpack._msgpack_numpy.has_numpy", False)
def test_msgpack_without_numpy():
    """Test that msgpack works without numpy and raises correct errors (e.g.
    when serializing datetime objects, the error should be msgpack's TypeError,
    not a "'np' is not defined error")."""
    with pytest.raises(TypeError):
        msgpack_loads(msgpack_dumps(datetime.datetime.now()))


def test_msgpack_custom_encoder_decoder():
    class CustomObject:
        def __init__(self, value):
            self.value = value

    def serialize_obj(obj, chain=None):
        if isinstance(obj, CustomObject):
            return {"__custom__": obj.value}
        return obj if chain is None else chain(obj)

    def deserialize_obj(obj, chain=None):
        if "__custom__" in obj:
            return CustomObject(obj["__custom__"])
        return obj if chain is None else chain(obj)

    data = {"a": 123, "b": CustomObject({"foo": "bar"})}
    with pytest.raises(TypeError):
        msgpack_dumps(data)

    # Register custom encoders/decoders to handle CustomObject
    msgpack_encoders.register("custom_object", func=serialize_obj)
    msgpack_decoders.register("custom_object", func=deserialize_obj)
    bytes_data = msgpack_dumps(data)
    new_data = msgpack_loads(bytes_data)
    assert new_data["a"] == 123
    assert isinstance(new_data["b"], CustomObject)
    assert new_data["b"].value == {"foo": "bar"}
    # Test that it also works with combinations of encoders/decoders (e.g. numpy)
    data = {"a": numpy.zeros((1, 2, 3)), "b": CustomObject({"foo": "bar"})}
    bytes_data = msgpack_dumps(data)
    new_data = msgpack_loads(bytes_data)
    assert isinstance(new_data["a"], numpy.ndarray)
    assert isinstance(new_data["b"], CustomObject)
    assert new_data["b"].value == {"foo": "bar"}
