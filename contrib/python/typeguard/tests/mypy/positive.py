from typing import Callable

from typeguard import typechecked


@typechecked
def foo(x: str) -> str:
    return "hello " + x


def takes_callable(f: Callable[[str], str]) -> str:
    return f("typeguard")


takes_callable(foo)


@typechecked
def has_valid_arguments(x: int, y: str) -> None:
    pass


def has_valid_return_type(y: str) -> str:
    return y


@typechecked
class MyClass:
    def __init__(self, x: int) -> None:
        self.x = x

    def add(self, y: int) -> int:
        return self.x + y


def get_value(c: MyClass) -> int:
    return c.x


@typechecked
def get_value_checked(c: MyClass) -> int:
    return c.x


def create_myclass(x: int) -> MyClass:
    return MyClass(x)


@typechecked
def create_myclass_checked(x: int) -> MyClass:
    return MyClass(x)


get_value(create_myclass(3))
get_value_checked(create_myclass_checked(1))
