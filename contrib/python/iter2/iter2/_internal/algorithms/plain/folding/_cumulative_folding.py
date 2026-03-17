import typing as tp

from ....lib.std import (
    itertools_chain,
    itertools_accumulate,
)

from ....lib.undefined import UNDEFINED
from ....lib.functions import Fn2


# ---

type HomoFoldFn[T] = Fn2[T, T, T]
type HeteroFoldFn[S, T] = Fn2[S, T, S]


@tp.overload
def fold_cumulative[Item](
    iterable: tp.Iterable[Item],
    fn: HomoFoldFn[Item],
) -> tp.Iterator[Item]: ...


@tp.overload
def fold_cumulative[State, Item](
    iterable: tp.Iterable[Item],
    initial: State,
    fn: HeteroFoldFn[State, Item],
) -> tp.Iterator[State]: ...


# ---

def fold_cumulative(iterable, arg1, arg2=UNDEFINED):  # type: ignore
    if UNDEFINED.is_not(arg2):
        return itertools_accumulate(
            itertools_chain(
                (arg1,),  # `initial`
                iterable,
            ),
            arg2,
        )
    else:
        return itertools_accumulate(
            iterable,
            arg1,  # `fn`
        )
