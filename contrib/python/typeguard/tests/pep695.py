from typeguard import typechecked


@typechecked
class ParametrizedClass[T]:
    def method(self, x: T, y: str) -> T:
        return x


@typechecked
def parametrized_func[T](x: T, y: str) -> T:
    return x
