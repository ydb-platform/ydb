import typing as t

import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_after() -> None:
    def func(a: int, b: t.Dict[str, int], c: bytes) -> str:
        return f"{a} {b} {c!r}"

    after_func = _.after(func, 1)
    reveal_type(after_func)  # R: pydash.functions.After[[a: builtins.int, b: builtins.dict[builtins.str, builtins.int], c: builtins.bytes], builtins.str]
    reveal_type(after_func(1, {}, b""))  # R: Union[builtins.str, None]


@pytest.mark.mypy_testing
def test_mypy_ary() -> None:
    def func(a: int, b: int, c: int = 0, d: int = 5) -> t.Tuple[int, int, int, int]:
        return (a, b, c, d)

    ary_func = _.ary(func, 2)
    reveal_type(ary_func(1, 2, 3, 4, 5, 6))  # R: Tuple[builtins.int, builtins.int, builtins.int, builtins.int]
    reveal_type(ary_func(1, 2, 3, 4, 5, 6, c=10, d=20))  # R: Tuple[builtins.int, builtins.int, builtins.int, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_before() -> None:
    def func(a: int, b: int, c: int) -> t.Tuple[int, int, int]:
        return (a, b, c)

    before_func = _.before(func, 3)
    reveal_type(before_func(1, 2, 3))  # R: Union[Tuple[builtins.int, builtins.int, builtins.int], None]


@pytest.mark.mypy_testing
def test_mypy_conjoin() -> None:
    def up_3(x: int) -> bool:
        return x > 3
    def is_int(x: t.Any) -> int:
        return isinstance(x, int)
    conjoiner = _.conjoin(is_int, up_3)

    reveal_type(conjoiner([1, 2, 3]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_curry() -> None:
    def func(a: int, b: str, c: bytes) -> t.Tuple[int, str, bytes]:
        return (a, b, c)

    currier = _.curry(func)
    reveal_type(currier)  # R: pydash.functions.CurryThree[builtins.int, builtins.str, builtins.bytes, Tuple[builtins.int, builtins.str, builtins.bytes]]

    currier1 = currier(1)
    reveal_type(currier1)  # R: pydash.functions.CurryTwo[builtins.str, builtins.bytes, Tuple[builtins.int, builtins.str, builtins.bytes]]

    currier2 = currier1("hi")
    reveal_type(currier2)  # R: pydash.functions.CurryOne[builtins.bytes, Tuple[builtins.int, builtins.str, builtins.bytes]]

    currier3 = currier2(b"hi again")
    reveal_type(currier3)  # R: Tuple[builtins.int, builtins.str, builtins.bytes]


@pytest.mark.mypy_testing
def test_mypy_curry_right() -> None:
    def func(a: int, b: str, c: bytes) -> t.Tuple[int, str, bytes]:
        return (a, b, c)

    currier = _.curry_right(func)
    reveal_type(currier)  # R: pydash.functions.CurryRightThree[builtins.bytes, builtins.str, builtins.int, Tuple[builtins.int, builtins.str, builtins.bytes]]

    currier1 = currier(b"hi again")
    reveal_type(currier1)  # R: pydash.functions.CurryRightTwo[builtins.str, builtins.int, Tuple[builtins.int, builtins.str, builtins.bytes]]

    currier2 = currier1("hi")
    reveal_type(currier2)  # R: pydash.functions.CurryRightOne[builtins.int, Tuple[builtins.int, builtins.str, builtins.bytes]]

    currier3 = currier2(1)
    reveal_type(currier3)  # R: Tuple[builtins.int, builtins.str, builtins.bytes]


@pytest.mark.mypy_testing
def test_mypy_debounce() -> None:
    def func(a: int, b: str) -> t.Tuple[int, str]:
        return (a, b)

    debounced = _.debounce(func, 5000)

    reveal_type(debounced)  # R: pydash.functions.Debounce[[a: builtins.int, b: builtins.str], Tuple[builtins.int, builtins.str]]
    reveal_type(debounced(5, "hi"))  # R: Tuple[builtins.int, builtins.str]


@pytest.mark.mypy_testing
def test_mypy_delay() -> None:
    def func(a: int, b: str) -> t.Tuple[int, str]:
        return (a, b)

    reveal_type(_.delay(func, 0, 5, "hi"))  # R: Tuple[builtins.int, builtins.str]


@pytest.mark.mypy_testing
def test_mypy_disjoin() -> None:
    def is_float(x: t.Any) -> bool:
        return isinstance(x, float)
    def is_int(x: t.Any) -> bool:
        return isinstance(x, int)
    disjoiner = _.disjoin(is_float, is_int)

    reveal_type(disjoiner)  # R: pydash.functions.Disjoin[Any]
    reveal_type(disjoiner([1, '2', '3']))  # R: builtins.bool
    reveal_type(disjoiner([1.0, '2', '3']))  # R: builtins.bool
    reveal_type(disjoiner(['1', '2', '3']))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_flip() -> None:
    def func(a: int, b: str, c: bytes) -> t.Tuple[int, str, bytes]:
        return (a, b, c)

    reveal_type(_.flip(func))  # R: def (builtins.bytes, builtins.str, builtins.int) -> Tuple[builtins.int, builtins.str, builtins.bytes]


@pytest.mark.mypy_testing
def test_mypy_flow() -> None:
    def mult_5(x: int) -> int:
        return x * 5
    def div_10(x: int) -> float:
        return x / 10.0
    def pow_2(x: float) -> float:
        return x ** 2
    def sum_list(x: t.List[int]) -> int:
        return sum(x)

    ops = _.flow(sum_list, mult_5, div_10, pow_2)
    reveal_type(ops)  # R: pydash.functions.Flow[[x: builtins.list[builtins.int]], builtins.float]
    reveal_type(ops([1, 2, 3, 4]))  # R: builtins.float


@pytest.mark.mypy_testing
def test_mypy_flow_right() -> None:
    def mult_5(x: float) -> float:
        return x * 5
    def div_10(x: int) -> float:
        return x / 10
    def pow_2(x: int) -> int:
        return x ** 2
    def sum_list(x: t.List[int]) -> int:
        return sum(x)

    ops = _.flow_right(mult_5, div_10, pow_2, sum_list)
    reveal_type(ops)  # R: pydash.functions.Flow[[x: builtins.list[builtins.int]], builtins.float]
    reveal_type(ops([1, 2, 3, 4]))  # R: builtins.float


@pytest.mark.mypy_testing
def test_mypy_iterated() -> None:
    def double(x: int) -> int:
        return x * 2

    doubler = _.iterated(double)
    reveal_type(doubler)  # R: pydash.functions.Iterated[builtins.int]

    reveal_type(doubler(4, 5))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_juxtapose() -> None:
    def double(x: int) -> int:
        return x * 2
    def triple(x: int) -> int:
        return x * 3
    def quadruple(x: int) -> int:
        return x * 4

    f = _.juxtapose(double, triple, quadruple)
    reveal_type(f)  # R: pydash.functions.Juxtapose[[x: builtins.int], builtins.int]
    reveal_type(f(5))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_negate() -> None:
    def is_number(x: t.Any) -> bool:
        return isinstance(x, (int, float))

    not_is_number = _.negate(is_number)
    reveal_type(not_is_number)  # R: pydash.functions.Negate[[x: Any]]
    reveal_type(not_is_number('1'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_once() -> None:
    def first_arg(*args: int) -> int:
        return args[0]

    oncer = _.once(first_arg)
    reveal_type(oncer)  # R: pydash.functions.Once[[*args: builtins.int], builtins.int]
    reveal_type(oncer(6))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_over_args() -> None:
    def squared(x: int) -> int:
        return x ** 2
    def double(x: int) -> int:
        return x * 2
    def in_list(x: int, y: int) -> t.List[int]:
        return [x, y]

    modder = _.over_args(in_list, squared, double)
    reveal_type(modder)  # R: def (builtins.int, builtins.int) -> builtins.list[builtins.int]
    reveal_type(modder(5, 10))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_partial() -> None:
    def cut(array: t.List[int], n: int) -> t.List[int]:
        return array[n:]

    dropper = _.partial(cut, [1, 2, 3, 4])
    reveal_type(dropper)  # R: pydash.functions.Partial[builtins.list[builtins.int]]
    reveal_type(dropper(2))  # R: builtins.list[builtins.int]

    myrest = _.partial(cut, n=1)
    reveal_type(myrest)  # R: pydash.functions.Partial[builtins.list[builtins.int]]
    reveal_type(myrest([1, 2, 3, 4]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_partial_right() -> None:
    def cut(array: t.List[int], n: int) -> t.List[int]:
        return array[n:]

    myrest = _.partial_right(cut, 1)
    reveal_type(myrest)  # R: pydash.functions.Partial[builtins.list[builtins.int]]
    reveal_type(myrest([1, 2, 3, 4]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_rearg() -> None:
    def func(x: int, y: int) -> t.List[int]:
        return [x, y]

    jumble = _.rearg(func, 1, 2, 3)
    reveal_type(jumble)  # R: pydash.functions.Rearg[[x: builtins.int, y: builtins.int], builtins.list[builtins.int]]
    reveal_type(jumble(1, 2))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_spread() -> None:
    def greet_people(*people: str) -> str:
        return 'Hello ' + ', '.join(people) + '!'

    greet = _.spread(greet_people)
    reveal_type(greet)  # R: pydash.functions.Spread[builtins.str]
    reveal_type(greet(['Mike', 'Don', 'Leo']))  # R: builtins.str

@pytest.mark.mypy_testing
def test_mypy_throttle() -> None:
    def func(x: int) -> int:
        return x

    throttled = _.throttle(func, 0)
    reveal_type(throttled)  # R: pydash.functions.Throttle[[x: builtins.int], builtins.int]
    reveal_type(throttled(5))  # R: builtins.int

@pytest.mark.mypy_testing
def test_mypy_unary() -> None:
    def func(a: int, b: int = 1, c: int = 0, d: int = 5) -> t.Tuple[int, int, int, int]:
        return (a, b, c, d)

    unary_func = _.unary(func)
    reveal_type(unary_func)  # R: pydash.functions.Ary[Tuple[builtins.int, builtins.int, builtins.int, builtins.int]]
    reveal_type(unary_func(1, 2, 3, 4, 5, 6))  # R: Tuple[builtins.int, builtins.int, builtins.int, builtins.int]
    reveal_type(unary_func(1, 2, 3, 4, 5, 6, c=10, d=20))  # R: Tuple[builtins.int, builtins.int, builtins.int, builtins.int]


@pytest.mark.mypy_testing
def test_mypy_wrap() -> None:
    def as_tuple(x: str, y: int) -> t.Tuple[str, int]:
        return (x, y)

    wrapper = _.wrap('hello', as_tuple)
    reveal_type(wrapper)  # R: pydash.functions.Partial[Tuple[builtins.str, builtins.int]]
    reveal_type(wrapper(1))  # R: Tuple[builtins.str, builtins.int]
