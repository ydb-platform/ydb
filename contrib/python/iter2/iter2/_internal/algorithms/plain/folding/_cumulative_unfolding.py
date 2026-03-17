import typing as tp

from ....lib.std import (
    builtin_zip,

    itertools_starmap,
    itertools_chain,
    itertools_tee,
)
from ....lib.undefined import UNDEFINED
from ....lib.functions import Fn2


# ---

type HomoFoldFn[T] = Fn2[T, T, T]
type HeteroFoldFn[S, T] = Fn2[S, T, S]


@tp.overload
def unfold_cumulative[Item](
    iterable: tp.Iterable[Item],
    fn: HomoFoldFn[Item],
) -> tp.Iterator[Item]: ...


@tp.overload
def unfold_cumulative[State, Item](
    iterable: tp.Iterable[Item],
    initial: State,
    fn: HeteroFoldFn[State, Item],
) -> tp.Iterator[Item]: ...


# ---

def unfold_cumulative(iterable, arg1, arg2=UNDEFINED):  # type: ignore
    it_1, it_2 = itertools_tee(iterable)

    if UNDEFINED.is_not(arg2):
        yield arg1  # `initial`
        it_from_first = itertools_chain((arg1,), it_1)
        it_from_second = it_2
        func = arg2
    else:
        try:
            yield next(it_2)
        except StopIteration:
            return
        it_from_first = it_1
        it_from_second = it_2
        func=arg1

    yield from itertools_starmap(
        func,
        builtin_zip(
            it_from_second,
            it_from_first,
        )
    )
