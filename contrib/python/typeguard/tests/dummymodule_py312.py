from typeguard import typechecked

type Foo = list[int]


@typechecked
def func_using_type_alias(x: Foo) -> int:
    return x[0]


@typechecked
def func_using_type_of_type_alias(x: type[Foo]) -> type:
    return x


@typechecked
class ParametrizedClass[T]:
    def method(self, x: T, y: str) -> T:
        return x


@typechecked
def parametrized_func[T](x: T, y: str) -> T:
    return x
