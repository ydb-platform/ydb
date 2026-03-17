from srsly._pickle_api import pickle_dumps, pickle_loads


def test_pickle_dumps():
    data = {"hello": "world", "test": 123}
    expected = [
        b"\x80\x04\x95\x1e\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x05hello\x94\x8c\x05world\x94\x8c\x04test\x94K{u.",
        b"\x80\x04\x95\x1e\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x04test\x94K{\x8c\x05hello\x94\x8c\x05world\x94u.",
        b"\x80\x02}q\x00(X\x04\x00\x00\x00testq\x01K{X\x05\x00\x00\x00helloq\x02X\x05\x00\x00\x00worldq\x03u.",
        b"\x80\x05\x95\x1e\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x05hello\x94\x8c\x05world\x94\x8c\x04test\x94K{u.",
    ]
    msg = pickle_dumps(data)
    assert msg in expected


def test_pickle_loads():
    msg = pickle_dumps({"hello": "world", "test": 123})
    data = pickle_loads(msg)
    assert len(data) == 2
    assert data["hello"] == "world"
    assert data["test"] == 123
