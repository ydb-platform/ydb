import typing as t

import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_at() -> None:
    reveal_type(_.at([1, 2, 3, 4], 0, 2))  # R: builtins.list[Union[builtins.int, None]]
    reveal_type(_.at({"a": 1, "b": 2, "c": 3, "d": 4}, "a", "c"))  # R: builtins.list[Union[builtins.int, None]]
    reveal_type(_.at({"a": 1, "b": 2, "c": {"d": {"e": 3}}}, "a", ["c", "d", "e"]))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_count_by() -> None:
    reveal_type(_.count_by([1, 2, 1, 2, 3, 4]))  # R: builtins.dict[builtins.int, builtins.int]
    reveal_type(_.count_by(["a", "A", "B", "b"], lambda x: x.lower()))  # R: builtins.dict[builtins.str, builtins.int]
    reveal_type(_.count_by({"a": 1, "b": 1, "c": 3, "d": 3}))  # R: builtins.dict[builtins.int, builtins.int]
    reveal_type(_.count_by({"5": 5, "6": 6}, lambda x: int(x)))  # R: builtins.dict[builtins.int, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_every() -> None:
    reveal_type(_.every([1, True, "hello"]))  # R: builtins.bool
    reveal_type(_.every([1, False, "hello"]))  # R: builtins.bool
    reveal_type(_.every([{"a": 1}, {"a": True}, {"a": "hello"}], "a"))  # R: builtins.bool
    reveal_type(_.every([{"a": 1}, {"a": False}, {"a": "hello"}], "a"))  # R: builtins.bool
    reveal_type(_.every([{"a": 1}, {"a": 1}], {"a": 1}))  # R: builtins.bool
    reveal_type(_.every([{"a": 1}, {"a": 2}], {"a": 1}))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_filter_() -> None:
    reveal_type(_.filter_([{"a": 1}, {"b": 2}, {"a": 1, "b": 3}], {"a": 1}))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]
    reveal_type(_.filter_([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_find() -> None:
    reveal_type(_.find([1, 2, 3, 4], lambda x: x >= 3))  # R: Union[builtins.int, None]
    reveal_type(_.find([{"a": 1}, {"b": 2}, {"a": 1, "b": 2}], {"a": 1}))  # R: Union[builtins.dict[builtins.str, builtins.int], None]


@pytest.mark.mypy_testing
def test_mypy_find_last() -> None:
    reveal_type(_.find_last([1, 2, 3, 4], lambda x: x >= 3))  # R: Union[builtins.int, None]
    reveal_type(_.find_last([{"a": 1}, {"b": 2}, {"a": 1, "b": 2}], {"a": 1}))  # R: Union[builtins.dict[builtins.str, builtins.int], None]


@pytest.mark.mypy_testing
def test_mypy_flat_map() -> None:
    def listify(n: int) -> t.List[t.List[int]]:
        return [[n, n]]

    reveal_type(_.flat_map([1, 2], listify))  # R: builtins.list[builtins.list[builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_flat_map_deep() -> None:
    reveal_type(_.flat_map_deep([1, 2], lambda n: [[n, n]]))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_flat_map_depth() -> None:
    reveal_type(_.flat_map_depth([1, 2], lambda n: [[n, n]], 1))  # R: builtins.list[Any]
    reveal_type(_.flat_map_depth([1, 2], lambda n: [[n, n]], 2))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_for_each() -> None:
    results = {}

    def cb(x):
        results[x] = x**2

    reveal_type(_.for_each([1, 2, 3, 4], cb))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_for_each_right() -> None:
    results = {"total": 1}

    def cb(x):
        results["total"] = x * results["total"]

    reveal_type(_.for_each_right([1, 2, 3, 4], cb))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_group_by() -> None:
    reveal_type(_.group_by([{"a": 1, "b": 2}, {"a": 3, "b": 4}], "a"))  # R: builtins.dict[Any, builtins.list[builtins.dict[builtins.str, builtins.int]]]
    reveal_type(_.group_by([{"a": 1, "b": 2}, {"a": 3, "b": 4}], lambda d: d == {"a": 1}))  # R: builtins.dict[builtins.bool, builtins.list[builtins.dict[builtins.str, builtins.int]]]


@pytest.mark.mypy_testing
def test_mypy_includes() -> None:
    reveal_type(_.includes([1, 2, 3, 4], 2))  # R: builtins.bool
    reveal_type(_.includes([1, 2, 3, 4], 2, from_index=2))  # R: builtins.bool
    reveal_type(_.includes({"a": 1, "b": 2, "c": 3, "d": 4}, 2))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_invoke_map() -> None:
    reveal_type(_.invoke_map([{"a": [{"b": 1}]}, {"a": [{"c": 2}]}], "a[0].items"))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_key_by() -> None:
    reveal_type(_.key_by([{"a": 1, "b": 2}, {"a": 3, "b": 4}], "a"))  # R: builtins.dict[Any, Any]
    reveal_type(_.key_by([{"a": 1, "b": 2}, {"a": 3, "b": 4}], lambda d: d["a"]))  # R: builtins.dict[builtins.int, builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_map_() -> None:
    reveal_type(_.map_([1, 2, 3, 4], str))  # R: builtins.list[builtins.str]
    reveal_type(_.map_([{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}], "a"))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_nest() -> None:
    reveal_type(_.nest({"a": 1}))  # R: Any
    reveal_type(_.nest([0, 1]))  # R: Any
    reveal_type(_.nest({"a": 1}, "a"))  # R: Any


@pytest.mark.mypy_testing
def test_mypy_order_by() -> None:
    reveal_type(_.order_by([{"a": 2, "b": 1}, {"a": 3, "b": 2}, {"a": 1, "b": 3}], ["b", "a"]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]
    reveal_type(_.order_by([{"a": 2, "b": 1}, {"a": 3, "b": 2}, {"a": 1, "b": 3}], ["a", "b"], [False, True]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_partition() -> None:
    reveal_type(_.partition([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.list[builtins.list[builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_pluck() -> None:
    reveal_type(_.pluck([{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}], "a"))  # R: builtins.list[Any]
    reveal_type(_.pluck([[[0, 1]], [[2, 3]], [[4, 5]]], "0.1"))  # R: builtins.list[Any]
    reveal_type(_.pluck([{"a": {"b": [0, 1]}}, {"a": {"b": [2, 3]}}], ["a", "b", 1]))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_reduce_() -> None:
    reveal_type(_.reduce_([1, 2, 3, 4], lambda total, x: total * x))  # R: builtins.int
    reveal_type(_.reduce_(["a", "b", "c"], lambda x, y: x + 1 if y == "b" else x, 1))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_reduce_right() -> None:
    reveal_type(_.reduce_right([1, 2, 3, 4], lambda total, x: total * x))  # R: builtins.int
    reveal_type(_.reduce_right(["a", "b", "c"], lambda x, y: x + 1 if y == "b" else x, 1))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_reductions() -> None:
    reveal_type(_.reductions([1, 2, 3, 4], lambda total, x: total * x))  # R: builtins.list[builtins.int]
    reveal_type(_.reductions(["a", "b", "c"], lambda x, y: x + 1 if y == "b" else x, 1))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_reductions_right() -> None:
    reveal_type(_.reductions_right([1, 2, 3, 4], lambda total, x: total * x))  # R: builtins.list[builtins.int]
    reveal_type(_.reductions_right(["a", "b", "c"], lambda x, y: x + 1 if y == "b" else x, 1))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_reject() -> None:
    reveal_type(_.reject([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.list[builtins.int]
    reveal_type(_.reject([{"a": 0}, {"a": 1}, {"a": 2}], "a"))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]
    reveal_type(_.reject([{"a": 0}, {"a": 1}, {"a": 2}], {"a": 1}))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_sample() -> None:
    reveal_type(_.sample([1, 2, 3, 4, 5]))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sample_size() -> None:
    reveal_type(_.sample_size([1, 2, 3, 4, 5], 2))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_shuffle() -> None:
    reveal_type(_.shuffle([1, 2, 3, 4]))  # R: builtins.list[builtins.int]
    reveal_type(_.shuffle({1: "a", 2: "b"}))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_size() -> None:
    reveal_type(_.size([1, 2, 3, 4]))  # R: builtins.int
    reveal_type(_.size({"a": 1, "b": 1}))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_some() -> None:
    reveal_type(_.some([False, True, 0]))  # R: builtins.bool
    reveal_type(_.some([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_sort_by() -> None:
    reveal_type(_.sort_by({"a": 2, "b": 3, "c": 1}))  # R: builtins.list[builtins.int]
    reveal_type(_.sort_by({"a": 2, "b": 3, "c": 1}, reverse=True))  # R: builtins.list[builtins.int]
    reveal_type(_.sort_by([{"a": 2}, {"a": 3}, {"a": 1}], "a"))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]
