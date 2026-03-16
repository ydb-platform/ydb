import typing as tp

from functools import wraps

from ....interfaces.iterator2 import Iterator2
from ....iterator_proxy import Iterator2Proxy


# ---

def make_iterator2_from_iterator_builder[**Arguments, Item](
    iterator_fn: tp.Callable[Arguments, tp.Iterator[Item]],
) -> tp.Callable[Arguments, Iterator2[Item]]:
    @wraps(iterator_fn)
    def _iterator2_fn(*args, **kwargs):
        return Iterator2Proxy(iterator_fn(*args, **kwargs))
    return _iterator2_fn


def make_iterator2_from_iterable_builder[**Arguments, Item](
    iterable_fn: tp.Callable[Arguments, tp.Iterable[Item]],
) -> tp.Callable[Arguments, Iterator2[Item]]:
    @wraps(iterable_fn)
    def _iterator2_fn(*args, **kwargs):
        return Iterator2Proxy(iter(iterable_fn(*args, **kwargs)))
    return _iterator2_fn
