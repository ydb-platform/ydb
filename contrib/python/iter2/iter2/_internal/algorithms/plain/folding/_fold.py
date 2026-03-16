import typing as tp

from ....lib.std import functools_reduce
from ....lib.undefined import UNDEFINED
from ....lib.functions import Thunk, Fn2
from ....lib.boxed_values import (
    Option2,
    None2,
    Some2,
)


# --- Signatures ---

type HomoFoldFn[T] = Fn2[T, T, T]
type HeteroFoldFn[S, T] = Fn2[S, T, S]


@tp.overload
def fold_items[Item](
    iterable: tp.Iterable[Item],
    fn: HomoFoldFn[Item],
) -> Option2[Item]:
    ...


@tp.overload
def fold_items[Item, State](
    iterable: tp.Iterable[Item],
    initial: State,
    fn: HeteroFoldFn[State, Item],
) -> State:
    ...


@tp.overload
def fold_items[Item, State](
    iterable: tp.Iterable[Item],
    *,
    initial_fn: Thunk[State],
    fn: HeteroFoldFn[State, Item],
) -> State:
    ...


# --- Implementation ---

def fold_items(  # type: ignore
    iterable,
    arg1=UNDEFINED, arg2=UNDEFINED,
    *,
    initial_fn=UNDEFINED,
    fn=UNDEFINED,
):
    if UNDEFINED.is_not(arg1):
        if UNDEFINED.is_not(arg2):
            return functools_reduce(
                arg2,
                iterable,
                arg1,  # `initial`
            )
        else:
            return _combine_items(
                iterable,
                arg1,  # type: ignore - `fn`
            )

    # keyword args case
    if not UNDEFINED.is_not(fn):
        raise ValueError('Argument `fn` is required')

    if UNDEFINED.is_not(initial_fn):
        return functools_reduce(
            fn,
            iterable,
            initial_fn(),
        )
    else:
        return _combine_items(
            iterable,
            fn,
        )


def _combine_items[Item](
    iterable: tp.Iterable[Item],
    fold_fn: Fn2[Item, Item, Item],
) -> Option2[Item]:
    try:
        it = iter(iterable)
        initial = next(it)
        return Some2(
            functools_reduce(
                fold_fn,
                it,
                initial,
            )
        )
    except StopIteration:
        return None2
