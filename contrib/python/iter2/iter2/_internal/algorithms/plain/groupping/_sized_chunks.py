import typing as tp

from ....lib.std import (
    builtin_zip,
    itertools_takewhile,
    itertools_batched,
    itertools_zip_longest,
    functools_partial,
)
from ....lib.undefined import UNDEFINED, MaybeUndefined2
from ....lib.functions import Thunk

from ..generating import (
    yield_item_repeatedly,
    repeatedly_call_forever,
)
from ..folding import collect_first_items


# ---

def iterate_in_chunks_of_at_most_size[Item](
    iterable: tp.Iterable[Item],
    *,
    max_size: int,
) -> tp.Iterator[tp.Tuple[Item, ...]]:
    if max_size < 1:
        raise ValueError('Chunk size must be positive integer between 1 and sys.maxsize')

    if max_size <= 1024:  # WARNING: Magick number 2^10 was considered OK for greedy preallocation.
        return _iterate_in_chunks_of_at_most_size__via_batched__without_size_check(iterable, max_size=max_size)
    else:
        return _iterate_in_chunks_of_at_most_size__via_islice__without_size_check(iterable, max_size=max_size)


def _iterate_in_chunks_of_at_most_size__via_batched__without_size_check[Item](
    iterable: tp.Iterable[Item],
    *,
    max_size: int,
) -> tp.Iterator[tp.Tuple[Item, ...]]:
    return itertools_batched(iterable, max_size)


def _iterate_in_chunks_of_at_most_size__via_islice__without_size_check[Item](
    iterable: tp.Iterable[Item],
    *,
    max_size: int,
) -> tp.Iterator[tp.Tuple[Item, ...]]:
    take_chunk_of_items = functools_partial(
        collect_first_items,
        iter(iterable),
        count=max_size,
    )
    return itertools_takewhile(len, repeatedly_call_forever(take_chunk_of_items))


# ---

def iterate_in_chunks_of_exact_size_and_drop_left[Item](
    iterable: tp.Iterable[Item],
    *,
    size: int,
) -> tp.Iterator[tp.Tuple[Item, ...]]:
    return (
        chunk
        for chunk in iterate_in_chunks_of_at_most_size(iterable, max_size=size)
        if len(chunk) == size  # assumed: len(chunk) <= size
    )


# ---

@tp.overload
def iterate_in_chunks_of_exact_size_with_padding[Item](
    iterable: tp.Iterable[Item],
    *,
    size: int,
) -> tp.Iterator[tp.Tuple[tp.Optional[Item], ...]]: ...


@tp.overload
def iterate_in_chunks_of_exact_size_with_padding[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    size: int,
    fill_value: FillItem,
) -> tp.Iterator[tp.Tuple[tp.Union[Item, FillItem], ...]]: ...


@tp.overload
def iterate_in_chunks_of_exact_size_with_padding[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    size: int,
    fill_value_fn: Thunk[FillItem],
) -> tp.Iterator[tp.Tuple[tp.Union[Item, FillItem], ...]]: ...


def iterate_in_chunks_of_exact_size_with_padding[Item, FillItem](  # type: ignore
    iterable: tp.Iterator[Item],
    *,
    size: int,
    fill_value: MaybeUndefined2[FillItem] = UNDEFINED,
    fill_value_fn: MaybeUndefined2[Thunk[FillItem]] = UNDEFINED,
) -> tp.Union[
    tp.Iterator[tp.Tuple[Item | FillItem, ...]],
    tp.Iterator[tp.Tuple[tp.Optional[Item], ...]],
]:
    if size < 1:
        raise ValueError('Chunk size must be positive integer between 1 and sys.maxsize')

    if UNDEFINED.is_not(fill_value_fn):
        if UNDEFINED.is_not(fill_value):
            raise ValueError('Arguments `fill_value` and `fill_value_fn` are both defined but are exclusive')
        else:
            return _iterate_in_chunks_of_exact_size_with_padding_with_fill_value_constructor(
                iterable, size=size,
                fill_value_fn=fill_value_fn,
            )
    else:
        if UNDEFINED.is_not(fill_value):
            return _iterate_in_chunks_of_exact_size_with_padding_with_constant_fill_value(iterable, size=size, fill_value=fill_value)
        else:
            return _iterate_in_chunks_of_exact_size_with_padding_with_constant_fill_value(
                iterable,
                size=size,
                fill_value=None,
            )


def _iterate_in_chunks_of_exact_size_with_padding_with_constant_fill_value[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    size: int,
    fill_value: FillItem,
) -> tp.Iterator[tp.Tuple[Item | FillItem, ...]]:
    for chunk in iterate_in_chunks_of_at_most_size(iterable, max_size=size):
        # assumed: len(chunk) <= size
        if len(chunk) == size:
            yield chunk
        else:
            yield (
                *chunk,
                *yield_item_repeatedly(
                    fill_value,
                    number_of_times=(
                        size - len(chunk)
                    )
                )
            )


def _iterate_in_chunks_of_exact_size_with_padding_with_fill_value_constructor[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    size: int,
    fill_value_fn: Thunk[FillItem],
) -> tp.Iterator[tp.Tuple[Item | FillItem, ...]]:
    for chunk in iterate_in_chunks_of_at_most_size(iterable, max_size=size):
        # assumed: len(chunk) <= size
        if len(chunk) == size:
            yield chunk
        else:
            yield (
                *chunk,
                *(
                    fill_value_fn()
                    for _ in range(size - len(chunk))
                )
            )


# ---

def _BADLY_IMPLEMENTED_VIA_ZIP__iterate_in_chunks_of_exact_size_and_drop_left[Item](
    iterable: tp.Iterable[Item],
    *,
    size: int,
) -> tp.Iterator[tp.Tuple[Item, ...]]:
    it_copies = (iter(iterable),) * size
    return builtin_zip(*it_copies)


def _BADLY_IMPLEMENTED_VIA_ZIP_LONGEST_iterate_in_chunks_of_exact_size_with_padding(iterable, *, size, fill_value=None):  # type: ignore - `itertools.zip_longest` is tricky for type checker
    it_copies = (iter(iterable),) * size
    return itertools_zip_longest(*it_copies, fillvalue=fill_value)
