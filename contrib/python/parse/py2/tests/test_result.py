import pytest

import parse


def test_fixed_access():
    r = parse.Result((1, 2), {}, None)
    assert r[0] == 1
    assert r[1] == 2
    with pytest.raises(IndexError):
        r[2]
    with pytest.raises(KeyError):
        r["spam"]


def test_slice_access():
    r = parse.Result((1, 2, 3, 4), {}, None)
    assert r[1:3] == (2, 3)
    assert r[-5:5] == (1, 2, 3, 4)
    assert r[:4:2] == (1, 3)
    assert r[::-2] == (4, 2)
    assert r[5:10] == ()


def test_named_access():
    r = parse.Result((), {"spam": "ham"}, None)
    assert r["spam"] == "ham"
    with pytest.raises(KeyError):
        r["ham"]
    with pytest.raises(IndexError):
        r[0]


def test_contains():
    r = parse.Result(("cat",), {"spam": "ham"}, None)
    assert "spam" in r
    assert "cat" not in r
    assert "ham" not in r
