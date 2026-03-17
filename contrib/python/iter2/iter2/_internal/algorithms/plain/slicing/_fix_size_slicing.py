import typing as tp

from ....lib.std import itertools_islice


# ---

def slice_items[Item](
    iterable: tp.Iterable[Item],
    *,
    start_from_idx: int = 0,
    step_size: int = 1,
    end_before_idx: tp.Optional[int] = None,
) -> tp.Iterator[Item]:
    if start_from_idx < 0:
        raise ValueError(f'Start must be positive integer between 1 and sys.maxsize. Got: {start_from_idx}')

    if step_size <= 0:
        raise ValueError(f'Step must be positive integer between 1 and sys.maxsize. Got: {step_size}')

    if end_before_idx is not None and end_before_idx <= 0:
        raise ValueError(f'End must be positive integer between 1 and sys.maxsize. Got: {end_before_idx}')

    return itertools_islice(iterable, start_from_idx, end_before_idx, step_size)


# ---

def drop_first_items_and_iterate_rest[Item](
    iterable: tp.Iterable[Item],
    *,
    count: int,
) -> tp.Iterator[Item]:
    if count <= 0:
        raise ValueError(f'Count must be positive integer between 1 and sys.maxsize. Got: {count}')

    return itertools_islice(iterable, count, None)


def iterate_through_first_items[Item](
    iterable: tp.Iterable[Item],
    *,
    count: int,
) -> tp.Iterator[Item]:
    if count <= 0:
        raise ValueError(f'Count must be positive integer between 1 and sys.maxsize. Got: {count}')

    return itertools_islice(iterable, count)


def iterate_with_fixed_step[Item](
    iterable: tp.Iterable[Item],
    *,
    step: int,
) -> tp.Iterator[Item]:
    if step <= 0:
        raise ValueError(f'Step must be positive integer between 1 and sys.maxsize. Got: {step}')

    return itertools_islice(iterable, None, None, step)
