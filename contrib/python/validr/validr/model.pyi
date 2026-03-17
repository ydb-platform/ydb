import typing

from .schema import Compiler


class ImmutableInstanceError(AttributeError):
    ...


M = typing.TypeVar('M')


@typing.overload
def modelclass(
    cls: typing.Type[M],
    *, compiler: Compiler = None,
    immutable: bool = False,
) -> typing.Type[M]:
    ...


@typing.overload
def modelclass(
    *, compiler: Compiler = None,
    immutable: bool = False,
) -> typing.Callable[[typing.Type[M]], typing.Type[M]]:
    ...


def fields(m: typing.Any) -> typing.Set[str]:
    ...


def asdict(
    m: typing.Any,
    *, keys: typing.Iterable[str] = None,
) -> typing.Dict[str, typing.Any]:
    ...
