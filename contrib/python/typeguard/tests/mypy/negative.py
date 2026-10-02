from typeguard import typechecked, typeguard_ignore


@typechecked
def foo(x: int) -> int:
    return x + 1


@typechecked
def bar(x: int) -> int:
    return str(x)  # noqa: E501 # error: Incompatible return value type (got "str", expected "int")  [return-value]


@typeguard_ignore
def non_typeguard_checked_func(x: int) -> int:
    return str(x)  # noqa: E501 # error: Incompatible return value type (got "str", expected "int")  [return-value]


@typechecked
def returns_str() -> str:
    return bar(0)  # noqa: E501 # error: Incompatible return value type (got "int", expected "str")  [return-value]


@typechecked
def arg_type(x: int) -> str:
    return True  # noqa: E501 # error: Incompatible return value type (got "bool", expected "str")  [return-value]


@typechecked
def ret_type() -> str:
    return True  # noqa: E501 # error: Incompatible return value type (got "bool", expected "str")  [return-value]


_ = arg_type(foo)  # noqa: E501 # error: Argument 1 to "arg_type" has incompatible type "Callable[[int], int]"; expected "int"  [arg-type]
_ = foo("typeguard")  # noqa: E501 # error: Argument 1 to "foo" has incompatible type "str"; expected "int"  [arg-type]


@typechecked
class MyClass:
    def __init__(self, x: int = 0) -> None:
        self.x = x

    def add(self, y: int) -> int:
        return self.x + y


def get_value(c: MyClass) -> int:
    return c.x


def create_myclass(x: int) -> MyClass:
    return MyClass(x)


_ = get_value("foo")  # noqa: E501 # error: Argument 1 to "get_value" has incompatible type "str"; expected "MyClass"  [arg-type]
_ = MyClass(returns_str())  # noqa: E501 # error: Argument 1 to "MyClass" has incompatible type "str"; expected "int"  [arg-type]
