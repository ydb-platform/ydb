import typing as tp

from ....lib.std import itertools_count


# ---

def count_from(
    first_value: int,
    *,
    step: int = 1,
) -> tp.Iterator[int]:
    return itertools_count(first_value, step)
