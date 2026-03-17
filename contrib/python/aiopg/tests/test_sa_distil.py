import pytest

from aiopg.sa.connection import _distill_params

pytest.importorskip("aiopg.sa")  # noqa


def test_distill_none():
    assert _distill_params(None, None) == []


def test_distill_no_multi_no_param():
    assert _distill_params((), {}) == []


def test_distill_dict_multi_none_param():
    assert _distill_params(None, {"foo": "bar"}) == [{"foo": "bar"}]


def test_distill_dict_multi_empty_param():
    assert _distill_params((), {"foo": "bar"}) == [{"foo": "bar"}]


def test_distill_single_dict():
    assert _distill_params(({"foo": "bar"},), {}) == [{"foo": "bar"}]


def test_distill_single_list_strings():
    assert _distill_params((["foo", "bar"],), {}) == [["foo", "bar"]]


def test_distill_single_list_tuples():
    v1 = _distill_params(([("foo", "bar"), ("bat", "hoho")],), {})
    v2 = [("foo", "bar"), ("bat", "hoho")]
    assert v1 == v2


def test_distill_single_list_tuple():
    v1 = _distill_params(([("foo", "bar")],), {})
    v2 = [("foo", "bar")]
    assert v1 == v2


def test_distill_multi_list_tuple():
    v1 = _distill_params(([("foo", "bar")], [("bar", "bat")]), {})
    v2 = ([("foo", "bar")], [("bar", "bat")])
    assert v1 == v2


def test_distill_multi_strings():
    assert _distill_params(("foo", "bar"), {}) == [("foo", "bar")]


def test_distill_single_list_dicts():
    v1 = _distill_params(([{"foo": "bar"}, {"foo": "hoho"}],), {})
    assert v1 == [{"foo": "bar"}, {"foo": "hoho"}]


def test_distill_single_string():
    assert _distill_params(("arg",), {}) == [["arg"]]


def test_distill_multi_string_tuple():
    v1 = _distill_params((("arg", "arg"),), {})
    assert v1 == [("arg", "arg")]
