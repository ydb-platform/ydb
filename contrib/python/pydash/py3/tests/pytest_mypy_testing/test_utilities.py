import typing as t

import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_attempt() -> None:
    def divide_by_zero(n: int) -> float:
        return n/0

    reveal_type(_.attempt(divide_by_zero, 1))  # R: Union[builtins.float, builtins.Exception]


@pytest.mark.mypy_testing
def test_mypy_cond() -> None:
    def is_1(n: int) -> bool:
        return n == 1
    func = _.cond(
        [
            (is_1, _.constant("matches 1")),
        ]
    )
    reveal_type(func)  # R: def (*Any, **Any) -> builtins.str


@pytest.mark.mypy_testing
def test_mypy_conforms() -> None:
    def higher_than_1(n: int) -> bool:
        return n > 1
    def is_0(n: int) -> bool:
        return n == 0

    reveal_type(_.conforms({'b': higher_than_1}))  # R: def (builtins.dict[builtins.str, builtins.int]) -> builtins.bool
    reveal_type(_.conforms([higher_than_1, is_0]))  # R: def (builtins.list[builtins.int]) -> builtins.bool


@pytest.mark.mypy_testing
def test_mypy_conforms_to() -> None:
    reveal_type(_.conforms_to({'b': 2}, {'b': lambda n: n > 1}))  # R: builtins.bool
    reveal_type(_.conforms_to({'b': 0}, {'b': lambda n: n > 1}))  # R: builtins.bool
    reveal_type(_.conforms_to([2, 0], [lambda n: n > 1, lambda n: n == 0]))  # R: builtins.bool
    reveal_type(_.conforms_to([0, 0], [lambda n: n > 1, lambda n: n == 0]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_constant() -> None:
    reveal_type(_.constant(3.14))  # R: def (*Any, **Any) -> builtins.float
    reveal_type(_.constant("hello"))  # R: def (*Any, **Any) -> builtins.str


@pytest.mark.mypy_testing
def test_mypy_default_to() -> None:
    reveal_type(_.default_to(1, 10))  # R: builtins.int
    reveal_type(_.default_to(None, 10))  # R: builtins.int
    reveal_type(_.default_to(None, None))  # R: None


@pytest.mark.mypy_testing
def test_mypy_default_to_any() -> None:
    n1, n2, n3 = 1, 10, 20
    reveal_type(_.default_to_any(n1, n2, n3))  # R: builtins.int
    reveal_type(_.default_to_any(None, n2, n3))  # R: builtins.int
    reveal_type(_.default_to_any(None, None, n3))  # R: builtins.int
    reveal_type(_.default_to_any(None, None, None))  # R: None


@pytest.mark.mypy_testing
def test_mypy_identity() -> None:
    reveal_type(_.identity(1))  # R: builtins.int
    reveal_type(_.identity(1, 2, 3))  # R: builtins.int
    reveal_type(_.identity())  # R: None


@pytest.mark.mypy_testing
def test_mypy_iteratee() -> None:
    def add(x: int, y: int) -> int:
        return x + y

    reveal_type(_.iteratee('data'))  # R: def (*Any, **Any) -> Any
    reveal_type(_.iteratee({'active': True})) # R: def (*Any, **Any) -> Any
    reveal_type(_.iteratee(add))  # R: def (x: builtins.int, y: builtins.int) -> builtins.int


@pytest.mark.mypy_testing
def test_mypy_matches() -> None:
    reveal_type(_.matches({'a': {'b': 2}}))  # R: def (Any) -> builtins.bool


@pytest.mark.mypy_testing
def test_mypy_matches_property() -> None:
    reveal_type(_.matches_property('a', 1))  # R: def (Any) -> builtins.bool
    reveal_type(_.matches_property(0, 1))  # R: def (Any) -> builtins.bool
    reveal_type(_.matches_property('a', 2))  # R: def (Any) -> builtins.bool


@pytest.mark.mypy_testing
def test_mypy_memoize() -> None:
    def add(x: int, y: int) -> int:
        return x + y

    def mul(x: int, y: int) -> int:
        return x * y

    memoized_add = _.memoize(add)
    reveal_type(memoized_add)  # R: pydash.utilities.MemoizedFunc[[x: builtins.int, y: builtins.int], builtins.int, builtins.str]
    reveal_type(memoized_add.cache)  # R: builtins.dict[builtins.str, builtins.int]

    memoized_add_resolv = _.memoize(add, mul)
    reveal_type(memoized_add_resolv)  # R: pydash.utilities.MemoizedFunc[[x: builtins.int, y: builtins.int], builtins.int, builtins.int]
    reveal_type(memoized_add_resolv.cache)  # R: builtins.dict[builtins.int, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_method() -> None:
    obj = {'a': {'b': [None, lambda x: x]}}
    reveal_type(_.method('a.b.1'))  # R: def (*Any, **Any) -> Any


@pytest.mark.mypy_testing
def test_mypy_method_of() -> None:
    obj = {'a': {'b': [None, lambda x: x]}}
    reveal_type(_.method_of(obj))  # R: def (*Any, **Any) -> Any


@pytest.mark.mypy_testing
def test_mypy_noop() -> None:
    reveal_type(_.noop(5, "hello", {}))  # R: None


@pytest.mark.mypy_testing
def test_mypy_nth_arg() -> None:
    reveal_type(_.nth_arg(1))  # R: def (*Any, **Any) -> Any


@pytest.mark.mypy_testing
def test_mypy_now() -> None:
    reveal_type(_.now())  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_over() -> None:
    def first(*args: int) -> int:
        return args[0]
    def second(*args: int) -> int:
        return args[1]

    reveal_type(_.over([first, second]))  # R: def (*args: builtins.int) -> builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_over_every() -> None:
    def is_two(x: int) -> bool:
        return x == 2
    def is_one(x: int) -> bool:
        return x == 1

    reveal_type(_.over_every([is_one, is_two]))  # R: def (x: builtins.int) -> builtins.bool


@pytest.mark.mypy_testing
def test_mypy_over_some() -> None:
    def is_two(x: int) -> bool:
        return x == 2
    def is_one(x: int) -> bool:
        return x == 1

    reveal_type(_.over_some([is_one, is_two]))  # R: def (x: builtins.int) -> builtins.bool


@pytest.mark.mypy_testing
def test_mypy_property_() -> None:
    reveal_type(_.property_('data'))  # R: def (Any) -> Any
    reveal_type(_.property_(0))  # R: def (Any) -> Any


@pytest.mark.mypy_testing
def test_mypy_properties() -> None:
    reveal_type(_.properties('a', 'b', ['c', 'd', 'e']))  # R: def (Any) -> Any


@pytest.mark.mypy_testing
def test_mypy_property_of() -> None:
    reveal_type(_.property_of({'a': 1, 'b': 2, 'c': 3}))  # R: def (Union[typing.Hashable, builtins.list[typing.Hashable]]) -> Any


@pytest.mark.mypy_testing
def test_mypy_random() -> None:
    reveal_type(_.random())  # R: builtins.int
    reveal_type(_.random(5, 10))  # R: builtins.int
    reveal_type(_.random(floating=True))  # R: builtins.float
    reveal_type(_.random(5.4, 10))  # R: builtins.float
    reveal_type(_.random(5, 10.0))  # R: builtins.float
    reveal_type(_.random(5.5, 10.0))  # R: builtins.float


@pytest.mark.mypy_testing
def test_mypy_range_() -> None:
    reveal_type(list(_.range_(5)))  # R: builtins.list[builtins.int]
    reveal_type(list(_.range_(1, 4)))  # R: builtins.list[builtins.int]
    reveal_type(list(_.range_(0, 6, 2)))  # R: builtins.list[builtins.int]
    reveal_type(list(_.range_(4, 1)))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_range_right() -> None:
    reveal_type(list(_.range_right(5)))  # R: builtins.list[builtins.int]
    reveal_type(list(_.range_right(1, 4)))  # R: builtins.list[builtins.int]
    reveal_type(list(_.range_right(0, 6, 2)))  # R: builtins.list[builtins.int]
    reveal_type(list(_.range_right(4, 1)))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_result() -> None:
    reveal_type(_.result({'a': 1, 'b': lambda: 2}, 'a'))  # R: Any
    reveal_type(_.result(None, 'a', None))  # R: None
    reveal_type(_.result(None, 'a', 5))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_retry() -> None:
    def add(x: int, y: int) -> int:
        return x + y

    r = _.retry(attempts=3, delay=0)
    reveal_type(r(add))  # R: def (x: builtins.int, y: builtins.int) -> builtins.int


@pytest.mark.mypy_testing
def test_mypy_stub_list() -> None:
    reveal_type(_.stub_list())  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_stub_dict() -> None:
    reveal_type(_.stub_dict())  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_stub_false() -> None:
    reveal_type(_.stub_false())  # R: Literal[False]


@pytest.mark.mypy_testing
def test_mypy_stub_string() -> None:
    reveal_type(_.stub_string())  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_stub_true() -> None:
    reveal_type(_.stub_true())  # R: Literal[True]


@pytest.mark.mypy_testing
def test_mypy_times() -> None:
    def to_str(x: int) -> str:
        return str(x)

    reveal_type(_.times(5))  # R: builtins.list[builtins.int]
    reveal_type(_.times(5, to_str))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_to_path() -> None:
    reveal_type(_.to_path('a.b.c'))  # R: builtins.list[typing.Hashable]


@pytest.mark.mypy_testing
def test_mypy_unique_id() -> None:
    reveal_type(_.unique_id())  # R: builtins.str
    reveal_type(_.unique_id('id_'))  # R: builtins.str
