from typing import Mapping


def find_index(predicate, iterable):
    for i, item in enumerate(iterable):
        if predicate(item):
            return i

    return -1


def merge(a: Mapping[str, str], b: Mapping[str, str]):
    return {**a, **b}
