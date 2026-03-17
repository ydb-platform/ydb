import typing as tp

from ....lib.std import (
    functools_partial,
    builtin_zip,
    collections_deque,
    itertools_tee,
)

from ..generating import cycle_through_items
from ..merging import iterate_sequentially_through
from ..folding import collect_first_items
from ..slicing import (
    iterate_through_first_items,
    drop_first_items_and_iterate_rest,
)


# ---

def iterate_with_sliding_window_of_at_most_size[Item](
    iterable: tp.Iterable[Item],
    *,
    size: int = 2,
    step: int = 1,
) -> tp.Iterator[tp.Sequence[Item]]:
    _assert_valid_window_size_and_step(
        size=size, step=step,
    )

    return _iterate_with_sliding_window_of_at_most_size__without_arg_checking(
        iterable,
        size=size,
        step=step,
    )


def _iterate_with_sliding_window_of_at_most_size__without_arg_checking[Item](
    iterable: tp.Iterable[Item],
    *,
    size: int = 2,
    step: int = 1,
) -> tp.Iterator[tp.Sequence[Item]]:
    it = iter(iterable)

    # --- first chunk ---

    buffer = collections_deque(
        iterate_through_first_items(it, count=size),
        maxlen=size,
    )

    first_piece_size = len(buffer)

    if first_piece_size > 0:
        yield tuple(buffer)

    if first_piece_size != size:
        return  # was last piece

    # --- main loop ---

    collect_diff_piece = functools_partial(
        collect_first_items, it, count=step,
    )

    while len(next_diff_piece := collect_diff_piece()) == step:
        buffer.extend(next_diff_piece)
        yield tuple(buffer)

    # --- last chunk ---

    if len(next_diff_piece) > 0:  # == step
        yield tuple(
            iterate_sequentially_through(
                drop_first_items_and_iterate_rest(iter(buffer), count=step),
                next_diff_piece,
            )
        )


# ---

def iterate_with_sliding_window_of_exact_size_with_padding[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    size: int = 2,
    step: int = 1,
    fill_value: FillItem,
) -> tp.Iterator[tp.Sequence[Item | FillItem]]:
    _assert_valid_window_size_and_step(
        size=size, step=step,
    )

    return _iterate_with_sliding_window_of_exact_size_with_padding__without_arg_checking(
        iterable,
        size=size, step=step,
        fill_value=fill_value,
    )


def _iterate_with_sliding_window_of_exact_size_with_padding__without_arg_checking[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    size: int = 2,
    step: int = 1,
    fill_value: FillItem,
) -> tp.Iterator[tp.Sequence[Item | FillItem]]:
    it = iter(iterable)
    buffer = collections_deque(
        iterate_through_first_items(it, count=size),
        maxlen=size,
    )

    first_piece_size = len(buffer)
    if first_piece_size == 0:
        return
    elif first_piece_size == size:
        yield tuple(buffer)
    else:  # 0 < first_piece_size < size
        yield tuple(
            iterate_sequentially_through(
                buffer,
                cycle_through_items(
                    (fill_value,),
                    number_of_times=(size - first_piece_size),
                ),
            )
        )
        return

    collect_diff_piece = functools_partial(
        collect_first_items, it, count=step,
    )

    while len(next_diff_piece := collect_diff_piece()) == step:
        buffer.extend(next_diff_piece)
        yield tuple(buffer)

    last_piece_size = len(next_diff_piece)

    if last_piece_size > 0:
        buffer.extend(
            iterate_sequentially_through(  # type: ignore - queue is Item, but (Item | FillItem)
                next_diff_piece,
                cycle_through_items(
                    (fill_value,),
                    number_of_times=(step - last_piece_size),
                ),
            )
        )
        yield tuple(buffer)


# ---

def iterate_pairwise[Item, FillItem](
    iterable: tp.Iterable[Item],
    *,
    fill_value: FillItem,
) -> tp.Iterator[tp.Tuple[Item, Item | FillItem]]:
    # --- tee ---
    it_1, it_2 = itertools_tee(iterable, 2)

    # --- first item ---
    try:
        first_item = next(it_2)
    except StopIteration:
        return

    # --- main generator ---
    it = builtin_zip(it_1, it_2)

    # --- first pair ---
    try:
        yield next(it)
    except StopIteration:
        yield (first_item, fill_value)
        return

    # --- rest ---
    yield from it


# ---

def _assert_valid_window_size_and_step(*, size: int, step: int):
    # NOTE: implies from next two checks
    # if size < 2:
    #     raise ValueError(
    #         f'Size of window must be >= 2: {size =}'
    #     )

    if step < 1:
        raise ValueError(
            f'Step must be >= 1: {step =}'
        )

    if step >= size:
        raise ValueError(
            f'Next window must overlap the previous (size > step): {size =}, {step =}'
        )
