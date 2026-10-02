from typing import Any

import pytest
from pytest import raises

from typeguard import TypeCheckError, check_argument_types, check_return_type


class TestCheckArgumentTypes:
    def test_success(self) -> None:
        def foo(x: int, /, y: str, *args: bool, z: bytes, **kwargs: int) -> None:
            check_argument_types()

        foo(1, "foo", True, False, z=b"foo", xyz=657, zzz=111)

    @pytest.mark.parametrize(
        "args, kwargs, pattern",
        [
            pytest.param(
                ("bar", "foo"),
                {"z": b"foo"},
                r'argument "x" \(str\) is not an instance of int',
                id="posonlyarg",
            ),
            pytest.param(
                (1, 1),
                {"z": b"foo"},
                r'argument "y" \(int\) is not an instance of str',
                id="posarg",
            ),
            pytest.param(
                (1, "foo"),
                {"z": "foo"},
                r'argument "z" \(str\) is not bytes-like',
                id="kwonlyarg",
            ),
            pytest.param(
                (1, "foo", 2),
                {"z": b"foo"},
                r'item 0 of argument "args" \(tuple\) is not an instance of bool',
                id="vararg",
            ),
            pytest.param(
                (1, "foo"),
                {"z": b"foo", "xyz": b"foo"},
                r"value of key 'xyz' of argument \"kwargs\" \(dict\) is not an instance of int",
                id="varkwarg",
            ),
        ],
    )
    def test_failure(
        self, args: tuple[Any], kwargs: dict[str, Any], pattern: str
    ) -> None:
        def foo(x: int, /, y: str, *args: bool, z: bytes, **kwargs: int) -> None:
            check_argument_types()

        with raises(TypeCheckError, match=pattern):
            foo(*args, **kwargs)


class TestCheckReturnType:
    def test_success(self) -> None:
        def foo() -> int:
            return check_return_type(0)

        foo()

    def test_failure(self) -> None:
        def foo() -> int:
            return check_return_type("foo")

        with raises(
            TypeCheckError, match=r"the return value \(str\) is not an instance of int"
        ):
            foo()
