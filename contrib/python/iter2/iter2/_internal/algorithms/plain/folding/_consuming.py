import typing as tp

from ....lib.std import collections_deque


# ---

def consume_iterator[Item](iterator: tp.Iterator[Item]) -> None:
    collections_deque(iterator, maxlen=0)  # fastest full consume ever
