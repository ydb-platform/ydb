import itertools
import keyword
import random
import string

from python_minifier.rename.util import builtins


def random_generator(length=40):
    valid_first = string.ascii_uppercase + string.ascii_lowercase
    valid_rest = string.digits + valid_first + '_'

    while True:
        first = [random.choice(valid_first)]
        rest = [random.choice(valid_rest) for i in range(length - 1)]
        yield ''.join(first + rest)


def name_generator():
    valid_first = string.ascii_uppercase + string.ascii_lowercase
    valid_rest = string.digits + valid_first + '_'

    for c in valid_first:
        yield c

    for length in itertools.count(1):
        for first in valid_first:
            for rest in itertools.product(valid_rest, repeat=length):
                name = first
                name += ''.join(rest)
                yield name


def name_filter():
    """
    Yield all valid python identifiers

    Name are returned sorted by length, then string sort order.

    Names that already have meaning in python (keywords and builtins)
    will not be included in the output.

    :rtype: Iterable[str]

    """

    reserved = keyword.kwlist + dir(builtins)

    for name in name_generator():
        if name not in reserved:
            yield name
