from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from hypothesis import strategies as st

if TYPE_CHECKING:
    from returns.primitives.laws import Lawful


def strategy_from_container(
    container_type: type[Lawful],
    *,
    use_init: bool = False,
) -> Callable[[type], st.SearchStrategy]:
    """
    Creates a strategy from a container type.

    Basically, containers should not support ``__init__``
    even when they have one.
    Because, that can be very complex: for example ``FutureResult`` requires
    ``Awaitable[Result[a, b]]`` as an ``__init__`` value.

    But, custom containers pass ``use_init``
    if they are not an instance of ``ApplicativeN``
    and do not have a working ``.from_value`` method.
    For example, pure ``MappableN`` can do that.

    We also try to resolve generic arguments.
    So, ``Result[_ValueType, Exception]``
    will produce any value for success cases
    and only exceptions for failure cases.
    """
    from returns.interfaces.applicative import ApplicativeN  # noqa: PLC0415
    from returns.interfaces.specific import maybe, result  # noqa: PLC0415

    def factory(type_: type) -> st.SearchStrategy:
        value_type, error_type = _get_type_vars(type_)

        strategies: list[st.SearchStrategy[Any]] = []
        if use_init and getattr(container_type, '__init__', None):
            strategies.append(st.builds(container_type))
        if issubclass(container_type, ApplicativeN):
            strategies.append(
                st.builds(
                    container_type.from_value,
                    st.from_type(value_type),
                )
            )
        if issubclass(container_type, result.ResultLikeN):
            strategies.append(
                st.builds(
                    container_type.from_failure,
                    st.from_type(error_type),
                )
            )
        if issubclass(container_type, maybe.MaybeLikeN):
            strategies.append(
                st.builds(
                    container_type.from_optional,
                    st.from_type(value_type),
                )
            )
        return st.one_of(*strategies)

    return factory


_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')


def _get_type_vars(thing: type):
    return getattr(thing, '__args__', (_FirstType, _SecondType))[:2]
