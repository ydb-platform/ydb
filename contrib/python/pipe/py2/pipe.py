#!/usr/bin/env python

"""Module enabling a sh like infix syntax (using pipes).
"""

import functools
import itertools
import socket
import sys
from contextlib import closing
from collections import deque
import warnings

try:
    import builtins
except ImportError:
    import __builtin__ as builtins


__author__ = "Julien Palard <julien@eeple.fr>"
__credits__ = """Jerome Schneider, for his Python skillz,
and dalexander for contributing"""
__date__ = "27 Jul 2018"
__version__ = "1.6.0"
__all__ = [
    "Pipe",
    "take",
    "tail",
    "skip",
    "all",
    "any",
    "average",
    "count",
    "map",
    "max",
    "min",
    "as_dict",
    "as_set",
    "permutations",
    "netcat",
    "netwrite",
    "traverse",
    "concat",
    "as_list",
    "as_tuple",
    "stdout",
    "lineout",
    "tee",
    "add",
    "first",
    "chain",
    "select",
    "where",
    "take_while",
    "skip_while",
    "aggregate",
    "groupby",
    "sort",
    "reverse",
    "chain_with",
    "islice",
    "izip",
    "passed",
    "index",
    "strip",
    "lstrip",
    "rstrip",
    "run_with",
    "t",
    "to_type",
    "transpose",
    "dedup",
    "uniq",
]


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

    def __init__(self, function):
        self.function = function
        functools.update_wrapper(self, function)

    def __ror__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        return Pipe(lambda x: self.function(x, *args, **kwargs))


@Pipe
def take(iterable, qte):
    "Yield qte of elements in the given iterable."
    for item in iterable:
        if qte > 0:
            qte -= 1
            yield item
        else:
            return


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


@Pipe
def all(iterable, pred):
    """Returns True if ALL elements in the given iterable are true for the
    given pred function"""
    warnings.warn(
        "pipe.all is deprecated, use the builtin all(...) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return builtins.all(pred(x) for x in iterable)


@Pipe
def any(iterable, pred):
    """Returns True if ANY element in the given iterable is True for the
    given pred function"""
    warnings.warn(
        "pipe.any is deprecated, use the builtin any(...) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return builtins.any(pred(x) for x in iterable)


@Pipe
def average(iterable):
    """Build the average for the given iterable, starting with 0.0 as seed
    Will try a division by 0 if the iterable is empty...
    """
    warnings.warn(
        "pipe.average is deprecated, use statistics.mean instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    total = 0.0
    qte = 0
    for element in iterable:
        total += element
        qte += 1
    return total / qte


@Pipe
def count(iterable):
    "Count the size of the given iterable, walking thrue it."
    warnings.warn(
        "pipe.count is deprecated, use the builtin len() instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    count = 0
    for element in iterable:
        count += 1
    return count


@Pipe
def max(iterable, **kwargs):
    warnings.warn(
        "pipe.max is deprecated, use the builtin max() instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return builtins.max(iterable, **kwargs)


@Pipe
def min(iterable, **kwargs):
    warnings.warn(
        "pipe.min is deprecated, use the builtin min() instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return builtins.min(iterable, **kwargs)


@Pipe
def as_dict(iterable):
    warnings.warn(
        "pipe.as_dict is deprecated, use dict(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return dict(iterable)


@Pipe
def as_set(iterable):
    warnings.warn(
        "pipe.as_set is deprecated, use set(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return set(iterable)


@Pipe
def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    for x in itertools.permutations(iterable, r):
        yield x


@Pipe
def netcat(to_send, host, port):
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
def netwrite(to_send, host, port):
    warnings.warn("pipe.netwite is deprecated.", DeprecationWarning, stacklevel=4)
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.connect((host, port))
        for data in to_send | traverse:
            s.send(data)


@Pipe
def traverse(args):
    for arg in args:
        try:
            if isinstance(arg, str):
                yield arg
            else:
                for i in arg | traverse:
                    yield i
        except TypeError:
            # not iterable --- output leaf
            yield arg


@Pipe
def concat(iterable, separator=", "):
    warnings.warn(
        "pipe.concat is deprecated, use ', '.join(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return separator.join(builtins.map(str, iterable))


@Pipe
def as_list(iterable):
    warnings.warn(
        "pipe.as_list is deprecated, use list(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return list(iterable)


@Pipe
def as_tuple(iterable):
    warnings.warn(
        "pipe.as_tuple is deprecated, use tuple(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return tuple(iterable)


@Pipe
def stdout(x):
    warnings.warn(
        "pipe.stdout is deprecated, use print(your | pipe, end='') instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    sys.stdout.write(str(x))


@Pipe
def lineout(x):
    warnings.warn(
        "pipe.lineout is deprecated, use print(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    sys.stdout.write(str(x) + "\n")


@Pipe
def tee(iterable):
    for item in iterable:
        sys.stdout.write(str(item) + "\n")
        yield item


@Pipe
def write(iterable, fname, glue="\n"):
    warnings.warn(
        "pipe.write is deprecated, a context manager and the open builtin instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    with open(fname, "w") as f:
        for item in iterable:
            f.write(str(item) + glue)


@Pipe
def add(x):
    warnings.warn(
        "pipe.add is deprecated, use sum(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return sum(x)


@Pipe
def first(iterable):
    warnings.warn(
        "pipe.first is deprecated, use next(iter(your | pipe)) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return next(iter(iterable))


@Pipe
def select(iterable, selector):
    return builtins.map(selector, iterable)


map = select


@Pipe
def where(iterable, predicate):
    return (x for x in iterable if (predicate(x)))


@Pipe
def take_while(iterable, predicate):
    return itertools.takewhile(predicate, iterable)


@Pipe
def skip_while(iterable, predicate):
    return itertools.dropwhile(predicate, iterable)


@Pipe
def aggregate(iterable, function, **kwargs):
    warnings.warn(
        "pipe.aggregate is deprecated, use functols.reduce(function, your | pipe) "
        "instead",
        DeprecationWarning,
        stacklevel=4,
    )
    if "initializer" in kwargs:
        return functools.reduce(function, iterable, kwargs["initializer"])
    return functools.reduce(function, iterable)


@Pipe
def groupby(iterable, keyfunc):
    return itertools.groupby(sorted(iterable, key=keyfunc), keyfunc)


@Pipe
def sort(iterable, **kwargs):
    return sorted(iterable, **kwargs)


@Pipe
def reverse(iterable):
    return reversed(iterable)


@Pipe
def passed(x):
    warnings.warn("pipe.passed is deprecated.", DeprecationWarning, stacklevel=4)
    pass


@Pipe
def index(iterable, value, start=0, stop=None):
    warnings.warn(
        "pipe.index is deprecated, use (your | pipe)[n] instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return iterable.index(value, start, stop or len(iterable))


@Pipe
def strip(iterable, chars=None):
    return iterable.strip(chars)


@Pipe
def rstrip(iterable, chars=None):
    return iterable.rstrip(chars)


@Pipe
def lstrip(iterable, chars=None):
    return iterable.lstrip(chars)


@Pipe
def run_with(iterable, func):
    warnings.warn(
        "pipe.run_with is deprecated, call the function with * or ** instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return (
        func(**iterable)
        if isinstance(iterable, dict)
        else func(*iterable)
        if hasattr(iterable, "__iter__")
        else func(iterable)
    )


@Pipe
def t(iterable, y):
    if hasattr(iterable, "__iter__") and not isinstance(iterable, str):
        return iterable + type(iterable)([y])
    return [iterable, y]


@Pipe
def to_type(x, t):
    warnings.warn(
        "pipe.to_type is deprecated, use the_type(your | pipe) instead.",
        DeprecationWarning,
        stacklevel=4,
    )
    return t(x)


@Pipe
def transpose(iterable):
    return list(zip(*iterable))


chain = Pipe(itertools.chain.from_iterable)
chain_with = Pipe(itertools.chain)
islice = Pipe(itertools.islice)

# Python 2 & 3 compatibility
if "izip" in dir(itertools):
    izip = Pipe(itertools.izip)
else:
    izip = Pipe(zip)

if __name__ == "__main__":
    import doctest

    doctest.testfile("README.md")
