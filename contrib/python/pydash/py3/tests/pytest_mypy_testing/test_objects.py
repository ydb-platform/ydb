import typing as t

import pytest

import pydash as _


class MyClass:
    def __init__(self) -> None:
        self.x = 5
        self.lst: t.List[int] = []

    def get_x(self) -> int:
        return self.x


@pytest.mark.mypy_testing
def test_mypy_assign() -> None:
    obj = {b"d": 5.5}
    reveal_type(_.assign(obj, {"a": 1}, {"b": 2}, {"c": 3}))  # R: builtins.dict[Union[builtins.bytes, builtins.str], Union[builtins.float, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_assign_with() -> None:
    def customizer(x: t.Union[int, None], y: int) -> float:
        return float(y) if x is None else float(x + y)

    reveal_type(_.assign_with({"a": 1}, {"b": 2}, {"a": 3}, customizer=customizer))  # R: builtins.dict[builtins.str, Union[builtins.int, builtins.float]]


@pytest.mark.mypy_testing
def test_mypy_callables() -> None:
    reveal_type(_.callables({"a": 1, "b": lambda: 2, "c": lambda: 3}))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_clone() -> None:
    reveal_type(_.clone({"hello": 5}))  # R: builtins.dict[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_clone_with() -> None:
    x: t.Dict[str, t.Union[int, t.Dict[str, int]]] = {"a": 1, "b": 2, "c": {"d": 3}}

    def cbk(v: t.Union[int, t.Dict[str, int]], k: t.Union[str, None]) -> t.Union[int, None]:
        return v + 2 if isinstance(v, int) and k else None

    reveal_type(_.clone_with(x, cbk))  # R: builtins.dict[builtins.str, Union[builtins.int, builtins.dict[builtins.str, builtins.int], None]]


@pytest.mark.mypy_testing
def test_mypy_clone_deep() -> None:
    x: t.Dict[str, t.Union[int, t.Dict[str, int]]] = {"a": 1, "b": 2, "c": {"d": 3}}
    reveal_type(_.clone_deep(x))  # R: builtins.dict[builtins.str, Union[builtins.int, builtins.dict[builtins.str, builtins.int]]]


@pytest.mark.mypy_testing
def test_mypy_clone_deep_with() -> None:
    x: t.Dict[str, t.Union[int, t.Dict[str, int]]] = {"a": 1, "b": 2, "c": {"d": 3}}

    def cbk(v: t.Union[int, t.Dict[str, int]], k: t.Union[str, None]) -> t.Union[int, None]:
        return v + 2 if isinstance(v, int) and k else None

    reveal_type(_.clone_deep_with(x, cbk))  # R: builtins.dict[builtins.str, Union[builtins.int, builtins.dict[builtins.str, builtins.int], None]]


@pytest.mark.mypy_testing
def test_mypy_defaults() -> None:
    obj = {"a": 1}
    reveal_type(_.defaults(obj, {"b": 2}, {"c": 3}, {"a": 4}))  # R: builtins.dict[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_defaults_deep() -> None:
    obj = {"a": {"b": 1}}
    reveal_type(_.defaults_deep(obj, {"a": {"b": 2, "c": 3}}))  # R: builtins.dict[builtins.str, builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_find_key() -> None:
    def is_one(x: int) -> bool:
        return x == 1

    reveal_type(_.find_key({"a": 1, "b": 2, "c": 3}, is_one))  # R: Union[builtins.str, None]
    reveal_type(_.find_key([1, 2, 3, 4], is_one))  # R: Union[builtins.int, None]


@pytest.mark.mypy_testing
def test_mypy_find_last_key() -> None:
    def is_one(x: int) -> bool:
        return x == 1

    reveal_type(_.find_last_key({"a": 1, "b": 2, "c": 3}, is_one))  # R: Union[builtins.str, None]
    reveal_type(_.find_last_key([1, 2, 3, 1], is_one))  # R: Union[builtins.int, None]


@pytest.mark.mypy_testing
def test_mypy_for_in() -> None:
    def cb(v: int, k: str) -> None:
        return None

    reveal_type(_.for_in({"a": 1, "b": 2, "c": 3}, cb))  # R: builtins.dict[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_for_in_right() -> None:
    def cb(v: int) -> None:
        return None

    reveal_type(_.for_in_right([1, 2, 3, 4], cb))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_get() -> None:
    reveal_type(_.get({}, "a.b.c"))  # R: Any
    reveal_type(_.get({"a": {"b": {"c": [1, 2, 3, 4]}}}, "a.b.c[1]"))  # R: Any
    reveal_type(_.get({"a": {"b": [0, {"c": [1, 2]}]}}, "a.b.1.c.2"))  # R: Any
    reveal_type(_.get(["a", "b"], 0))  # R: Union[builtins.str, None]
    reveal_type(_.get(["a", "b"], 0, "c"))  # R: builtins.str
    reveal_type(_.get(MyClass(), "x"))  # R: Any


@pytest.mark.mypy_testing
def test_mypy_has() -> None:
    reveal_type(_.has([1, 2, 3], 1))  # R: builtins.bool
    reveal_type(_.has({"a": 1, "b": 2}, "b"))  # R: builtins.bool
    reveal_type(_.has(MyClass(), "get_x"))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_invert() -> None:
    reveal_type(_.invert({"a": 1, "b": 2, "c": 3}))  # R: builtins.dict[builtins.int, builtins.str]


@pytest.mark.mypy_testing
def test_mypy_invert_by() -> None:
    obj = {"a": 1, "b": 2, "c": 1}
    reveal_type(_.invert_by(obj))  # R: builtins.dict[builtins.int, builtins.list[builtins.str]]

    def group_prefix(x: int) -> str:
        return "group" + str(x)

    reveal_type(_.invert_by(obj, group_prefix))  # R: builtins.dict[builtins.str, builtins.list[builtins.str]]


@pytest.mark.mypy_testing
def test_mypy_invoke() -> None:
    obj = {"a": [{"b": {"c": [1, 2, 3, 4]}}]}
    reveal_type(_.invoke(obj, "a[0].b.c.pop", 1))  # R: Any
    reveal_type(_.invoke(MyClass(), "get_x"))  # R: Any


@pytest.mark.mypy_testing
def test_mypy_keys() -> None:
    reveal_type(_.keys([1, 2, 3]))  # R: builtins.list[builtins.int]
    reveal_type(_.keys({"a": 1, "b": 2}))  # R: builtins.list[builtins.str]
    reveal_type(_.keys(MyClass()))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_map_keys() -> None:
    def callback(value: int, key: str) -> str:
        return key * 2

    reveal_type(_.map_keys({"a": 1, "b": 2, "c": 3}, callback))  # R: builtins.dict[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_map_values() -> None:
    def times_two(x: int) -> str:
        return str(x * 2)

    reveal_type(_.map_values({"a": 1, "b": 2, "c": 3}, times_two))  # R: builtins.dict[builtins.str, builtins.str]
    reveal_type(_.map_values({"a": 1, "b": {"d": 4}, "c": 3}, {"d": 4}))  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_map_values_deep() -> None:
    x = {"a": 1, "b": {"c": 2}}

    def times_two(x: int) -> int:
        return x * 2

    reveal_type(_.map_values_deep(x, times_two))  # R: Any


@pytest.mark.mypy_testing
def test_mypy_merge() -> None:
    obj = {"a": 2}
    reveal_type(_.merge(obj, {"a": 1}, {"b": 2, "c": 3}, {"d": 4}))  # R: builtins.dict[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_merge_with() -> None:
    cbk = lambda obj_val, src_val: obj_val + src_val
    obj1 = {"a": [1], "b": [2]}
    obj2 = {"a": [3], "b": [4]}
    reveal_type(_.merge_with(obj1, obj2, cbk))  # R: Any


@pytest.mark.mypy_testing
def test_mypy_omit() -> None:
    reveal_type(_.omit({"a": 1, "b": 2, "c": 3}, "b", "c"))  # R: builtins.dict[builtins.str, builtins.int]
    reveal_type(_.omit({"a": 1, "b": 2, "c": 3}, ["a", "c"]))  # R: builtins.dict[builtins.str, builtins.int]
    reveal_type(_.omit([1, 2, 3, 4], 0, 3))  # R: builtins.dict[builtins.int, builtins.int]
    reveal_type(_.omit({"a": {"b": {"c": "d"}}}, "a.b.c"))  # R: builtins.dict[builtins.str, builtins.dict[builtins.str, builtins.dict[builtins.str, builtins.str]]]
    reveal_type(_.omit(MyClass(), "x"))  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_omit_by() -> None:
    def is_int(v: t.Union[str, int]) -> bool:
        return isinstance(v, int)

    obj: t.Dict[str, t.Union[str, int]] = {"a": 1, "b": "2", "c": 3}
    reveal_type(_.omit_by(obj, is_int))  # R: builtins.dict[builtins.str, Union[builtins.str, builtins.int]]
    reveal_type(_.omit_by([1, 2, 3, 4], is_int))  # R: builtins.dict[builtins.int, builtins.int]
    reveal_type(_.omit_by(MyClass(), is_int))  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_parse_int() -> None:
    reveal_type(_.parse_int("5"))  # R: Union[builtins.int, None]
    reveal_type(_.parse_int("12", 8))  # R: Union[builtins.int, None]


@pytest.mark.mypy_testing
def test_mypy_pick() -> None:
    reveal_type(_.pick({"a": 1, "b": 2, "c": 3}, "a", "b"))  # R: builtins.dict[builtins.str, builtins.int]
    reveal_type(_.pick(MyClass(), "x"))  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_pick_by() -> None:
    def is_int(v: t.Union[int, str]) -> bool:
        return isinstance(v, int)

    obj: t.Dict[str, t.Union[int, str]] = {"a": 1, "b": "2", "c": 3}

    reveal_type(_.pick_by(obj, is_int))  # R: builtins.dict[builtins.str, Union[builtins.int, builtins.str]]
    reveal_type(_.pick(MyClass(), lambda v: isinstance(v, int)))  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_rename_keys() -> None:
    reveal_type(_.rename_keys({"a": 1, "b": 2, "c": 3}, {"a": "A", "b": "B"}))  # R: builtins.dict[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_set_() -> None:
    reveal_type(_.set_({}, "a.b.c", 1))  # R: builtins.dict[Never, Never]
    reveal_type(_.set_(MyClass(), "x", 10))  # R: tests.pytest_mypy_testing.test_objects.MyClass


@pytest.mark.mypy_testing
def test_mypy_set_with() -> None:
    reveal_type(_.set_with({}, "[0][1]", "a", lambda: {}))  # R: builtins.dict[Never, Never]
    reveal_type(_.set_with(MyClass(), "x", lambda: 10))  # R: tests.pytest_mypy_testing.test_objects.MyClass


@pytest.mark.mypy_testing
def test_mypy_to_boolean() -> None:
    reveal_type(_.to_boolean("true"))  # R: Union[builtins.bool, None]


@pytest.mark.mypy_testing
def test_mypy_to_dict() -> None:
    obj = {"a": 1, "b": 2}
    reveal_type(_.to_dict(obj))  # R: builtins.dict[builtins.str, builtins.int]
    reveal_type(_.to_dict(MyClass()))  # R: builtins.dict[Any, Any]
    reveal_type(_.to_dict([1, 2, 3, 4]))  # R: builtins.dict[builtins.int, builtins.int]
    reveal_type(_.to_dict([(1, 2), (3, 4)]))  # R: builtins.dict[builtins.int, Tuple[builtins.int, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_to_integer() -> None:
    reveal_type(_.to_integer(3.2))  # R: builtins.int
    reveal_type(_.to_integer("3.2"))  # R: builtins.int
    reveal_type(_.to_integer("invalid"))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_to_list() -> None:
    reveal_type(_.to_list({"a": 1, "b": 2, "c": 3}))  # R: builtins.list[builtins.int]
    reveal_type(_.to_list((1, 2, 3, 4)))  # R: builtins.list[builtins.int]
    reveal_type(_.to_list(1))  # R: builtins.list[builtins.int]
    reveal_type(_.to_list([1]))  # R: builtins.list[builtins.int]
    reveal_type(_.to_list(a for a in [1, 2, 3]))  # R: builtins.list[builtins.int]
    reveal_type(_.to_list("cat"))  # R: builtins.list[builtins.str]
    reveal_type(_.to_list("cat", split_strings=False))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_to_number() -> None:
    reveal_type(_.to_number("1234.5678"))  # R: Union[builtins.float, None]
    reveal_type(_.to_number("1234.5678", 4))  # R: Union[builtins.float, None]
    reveal_type(_.to_number(1, 2))  # R: Union[builtins.float, None]


@pytest.mark.mypy_testing
def test_mypy_to_pairs() -> None:
    reveal_type(_.to_pairs([1, 2, 3, 4]))  # R: builtins.list[Tuple[builtins.int, builtins.int]]
    reveal_type(_.to_pairs({"a": 1}))  # R: builtins.list[Tuple[builtins.str, builtins.int]]
    reveal_type(_.to_pairs(MyClass()))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_to_string() -> None:
    reveal_type(_.to_string(1))  # R: builtins.str
    reveal_type(_.to_string(None))  # R: builtins.str
    reveal_type(_.to_string([1, 2, 3]))  # R: builtins.str
    reveal_type(_.to_string("a"))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_transform() -> None:
    def build_list(acc: t.List[t.Tuple[int, int]], v: int, k: int) -> None:
        return acc.append((k, v))

    base_list: t.List[t.Tuple[int, int]] = []
    reveal_type(_.transform([1, 2, 3, 4], build_list, base_list))  # R: builtins.list[Tuple[builtins.int, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_update() -> None:
    reveal_type(_.update({}, ["a", "b"], lambda value: value))  # R: builtins.dict[Any, Any]
    reveal_type(_.update([], [0, 0], lambda value: 1))  # R: builtins.list[Any]
    reveal_type(_.update(MyClass(), "x", lambda value: 10))  # R: tests.pytest_mypy_testing.test_objects.MyClass


@pytest.mark.mypy_testing
def test_mypy_update_with() -> None:
    reveal_type(_.update_with({}, "[0][1]", lambda x: "a", lambda x: {}))  # R: builtins.dict[Any, Any]
    reveal_type(_.update_with([], [0, 0], lambda x: 1, lambda x: []))  # R: builtins.list[Any]
    reveal_type(_.update_with(MyClass(), "lst.0", lambda value: 10, lambda x: []))  # R: tests.pytest_mypy_testing.test_objects.MyClass


@pytest.mark.mypy_testing
def test_mypy_unset() -> None:
    reveal_type(_.unset({"a": [{"b": {"c": 7}}]}, "a[0].b.c"))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_values() -> None:
    reveal_type(_.values({"a": "a", "b": "b", "c": "c"}))  # R: builtins.list[builtins.str]
    reveal_type(_.values({"a": 1, "b": 2, "c": 3}))  # R: builtins.list[builtins.int]
    reveal_type(_.values([2, 4, 6, 8]))  # R: builtins.list[builtins.int]
    reveal_type(_.values(MyClass()))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_apply() -> None:
    reveal_type(_.apply("1", lambda x: int(x)))  # R: builtins.int
    reveal_type(_.apply(1, lambda x: x + 1))  # R: builtins.int
    reveal_type(_.apply("hello", lambda x: x.upper()))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_apply_if() -> None:
    reveal_type(_.apply_if("5", lambda x: int(x), lambda x: x.isdecimal()))  # R: Union[builtins.str, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_apply_if_not_none() -> None:
    reveal_type(_.apply_if_not_none(1, lambda x: x + 1))  # R: Union[builtins.int, None]
    reveal_type(_.apply_if_not_none(None, lambda x: x + 1))  # R: Union[builtins.int, None]
    reveal_type(_.apply_if_not_none("hello", lambda x: x.upper()))  # R: Union[builtins.str, None]


@pytest.mark.mypy_testing
def test_mypy_apply_catch() -> None:
    reveal_type(_.apply_catch(5, lambda x: x / 0, [ZeroDivisionError]))  # R: Union[builtins.int, builtins.float]
    reveal_type(_.apply_catch(5, lambda x: x / 0, [ZeroDivisionError], "error"))  # R: Union[builtins.float, builtins.str]
