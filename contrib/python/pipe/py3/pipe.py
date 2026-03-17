"""Library allowing a sh like infix syntax using pipes."""

__author__ = "Julien Palard <julien@python.org>"
__version__ = "2.2"
__credits__ = """Jérôme Schneider for teaching me the Python datamodel,
and all contributors."""

import functools
import itertools
import socket
import sys
from contextlib import closing
from collections import deque
import builtins


class Pipe:
    """
    Represent a Pipeable Element :
    Described as :
    first = Pipe(lambda iterable: next(iter(iterable)))
    and used as :
    print [1, 2, 3] | first
    printing 1

    Or represent a Pipeable Function :
    It's a function returning a Pipe
    Described as :
    select = Pipe(lambda iterable, pred: (pred(x) for x in iterable))
    and used as :
    print [1, 2, 3] | select(lambda x: x * 2)
    # 2, 4, 6
    """

    def __init__(self, function, *args, **kwargs):
        self.function = lambda iterable, *args2, **kwargs2: function(
            iterable, *args, *args2, **kwargs, **kwargs2
        )
        functools.update_wrapper(self, function)

    def __ror__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        return Pipe(
            lambda iterable, *args2, **kwargs2: self.function(
                iterable, *args, *args2, **kwargs, **kwargs2
            )
        )


@Pipe
def take(iterable, qte):
    "Yield qte of elements in the given iterable."
    if not qte:
        return
    for item in iterable:
        yield item
        qte -= 1
        if qte == 0:
            break


@Pipe
def tail(iterable, qte):
    "Yield qte of elements in the given iterable."
    return deque(iterable, maxlen=qte)


@Pipe
def skip(iterable, qte):
    "Skip qte elements in the given iterable, then yield others."
    for item in iterable:
        if qte == 0:
            yield item
        else:
            qte -= 1


@Pipe
def dedup(iterable, key=lambda x: x):
    """Only yield unique items. Use a set to keep track of duplicate data."""
    seen = set()
    for item in iterable:
        dupkey = key(item)
        if dupkey not in seen:
            seen.add(dupkey)
            yield item


@Pipe
def uniq(iterable, key=lambda x: x):
    """Deduplicate consecutive duplicate values."""
    iterator = iter(iterable)
    try:
        prev = next(iterator)
    except StopIteration:
        return
    yield prev
    prevkey = key(prev)
    for item in iterator:
        itemkey = key(item)
        if itemkey != prevkey:
            yield item
        prevkey = itemkey


enumerate = Pipe(builtins.enumerate)


@Pipe
def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    yield from itertools.permutations(iterable, r)


@Pipe
def netcat(to_send, host, port):
    """Send and receive bytes over TCP."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.connect((host, port))
        for data in to_send | traverse:
            s.send(data)
        while 1:
            data = s.recv(4096)
            if not data:
                break
            yield data


@Pipe
def traverse(args):
    if isinstance(args, (str, bytes)):
        yield args
        return
    for arg in args:
        try:
            yield from arg | traverse
        except TypeError:
            # not iterable --- output leaf
            yield arg


@Pipe
def tee(iterable):
    for item in iterable:
        sys.stdout.write(repr(item) + "\n")
        yield item


@Pipe
def select(iterable, selector):
    return builtins.map(selector, iterable)


map = select


@Pipe
def where(iterable, predicate):
    return (x for x in iterable if predicate(x))


filter = where


@Pipe
def take_while(iterable, predicate):
    return itertools.takewhile(predicate, iterable)


@Pipe
def skip_while(iterable, predicate):
    return itertools.dropwhile(predicate, iterable)


@Pipe
def groupby(iterable, keyfunc):
    return itertools.groupby(sorted(iterable, key=keyfunc), keyfunc)


@Pipe
def sort(iterable, key=None, reverse=False):  # pylint: disable=redefined-outer-name
    return sorted(iterable, key=key, reverse=reverse)


@Pipe
def reverse(iterable):
    return reversed(iterable)


@Pipe
def t(iterable, y):
    if hasattr(iterable, "__iter__") and not isinstance(iterable, str):
        return iterable + type(iterable)([y])
    return [iterable, y]


@Pipe
def transpose(iterable):
    return list(zip(*iterable))


@Pipe
def batched(iterable, n):
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        yield batch


chain = Pipe(itertools.chain.from_iterable)
chain_with = Pipe(itertools.chain)
islice = Pipe(itertools.islice)
izip = Pipe(zip)

if __name__ == "__main__":
    import doctest

    doctest.testfile("README.md")
