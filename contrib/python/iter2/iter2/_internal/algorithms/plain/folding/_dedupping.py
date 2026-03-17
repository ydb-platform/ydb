import typing as tp

from ....lib.std import itertools_groupby

from ._consuming import consume_iterator


# ---

def deduplicate_same_consecutive_items[Item](iterable: tp.Iterable[Item]) -> tp.Iterator[Item]:
    for val, sub_iter in itertools_groupby(iterable):
        yield val
        consume_iterator(sub_iter)
