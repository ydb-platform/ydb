from collections.abc import Iterable
from typing import Any, Callable, Optional, TypeVar, overload

from ...common import TypeHint
from ...provider.essential import Provider
from .retort import ConversionRetort

_global_retort = ConversionRetort()

SrcT = TypeVar("SrcT")
DstT = TypeVar("DstT")
CallableT = TypeVar("CallableT", bound=Callable)


def convert(src_obj: Any, dst: type[DstT], *, recipe: Iterable[Provider] = ()) -> DstT:
    """Function transforming a source object to destination.

    :param src_obj: A type of converter input data.
    :param dst: A type of converter output data.
    :param recipe: An extra recipe adding to retort.
    :return: Instance of destination
    """
    return _global_retort.convert(src_obj, dst, recipe=recipe)


@overload
def get_converter(
    src: type[SrcT],
    dst: type[DstT],
    *,
    recipe: Iterable[Provider] = (),
    name: Optional[str] = None,
) -> Callable[[SrcT], DstT]:
    ...


@overload
def get_converter(
    src: TypeHint,
    dst: TypeHint,
    *,
    recipe: Iterable[Provider] = (),
    name: Optional[str] = None,
) -> Callable[[Any], Any]:
    ...


def get_converter(src: TypeHint, dst: TypeHint, *, recipe: Iterable[Provider] = (), name: Optional[str] = None):
    """Factory producing basic converter.

    :param src: A type of converter input data.
    :param dst: A type of converter output data.
    :param recipe: An extra recipe adding to retort.
    :param name: Name of generated function, if value is None, name will be derived.
    :return: Desired converter function
    """
    return _global_retort.get_converter(src, dst, recipe=recipe, name=name)


@overload
def impl_converter(func_stub: CallableT, /) -> CallableT:
    ...


@overload
def impl_converter(*, recipe: Iterable[Provider] = ()) -> Callable[[CallableT], CallableT]:
    ...


def impl_converter(stub_function: Optional[Callable] = None, *, recipe: Iterable[Provider] = ()):
    """Decorator producing converter with signature of stub function.

    :param stub_function: A function that signature is used to generate converter.
    :param recipe: An extra recipe adding to retort.
    :return: Desired converter function
    """
    if stub_function is None:
        return _global_retort.impl_converter(recipe=recipe)
    return _global_retort.impl_converter(stub_function)
