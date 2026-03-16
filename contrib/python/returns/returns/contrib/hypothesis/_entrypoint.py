"""
Used to register all our types as hypothesis strategies.

See: https://hypothesis.readthedocs.io/en/latest/strategies.html

But, beware that we only register concrete types here,
interfaces won't be registered!

"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from returns.primitives.laws import Lawful

_Inst = TypeVar('_Inst', bound='Lawful')


def _setup_hook() -> None:
    from hypothesis import strategies as st  # noqa: PLC0415

    from returns.context import (  # noqa: PLC0415
        RequiresContext,
        RequiresContextFutureResult,
        RequiresContextIOResult,
        RequiresContextResult,
    )
    from returns.future import Future, FutureResult  # noqa: PLC0415
    from returns.io import IO, IOResult  # noqa: PLC0415
    from returns.maybe import Maybe  # noqa: PLC0415
    from returns.result import Result  # noqa: PLC0415

    def factory(
        container_type: type[_Inst],
    ) -> Callable[[Any], st.SearchStrategy[_Inst]]:
        def decorator(thing: Any) -> st.SearchStrategy[_Inst]:
            from returns.contrib.hypothesis.containers import (  # noqa: PLC0415
                strategy_from_container,
            )

            return strategy_from_container(container_type)(thing)

        return decorator

    #: Our types that we register in hypothesis
    #: to be working with ``st.from_type``
    registered_types: Sequence[type[Lawful]] = (
        Result,
        Maybe,
        IO,
        IOResult,
        Future,
        FutureResult,
        RequiresContext,
        RequiresContextResult,
        RequiresContextIOResult,
        RequiresContextFutureResult,
    )

    for type_ in registered_types:
        st.register_type_strategy(type_, factory(type_))
