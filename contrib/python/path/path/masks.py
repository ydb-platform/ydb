import re
import functools
import operator
import itertools


# from jaraco.functools
def compose(*funcs):
    compose_two = lambda f1, f2: lambda *args, **kwargs: f1(f2(*args, **kwargs))  # noqa
    return functools.reduce(compose_two, funcs)


# from jaraco.structures.binary
def gen_bit_values(number):
    """
    Return a zero or one for each bit of a numeric value up to the most
    significant 1 bit, beginning with the least significant bit.

    >>> list(gen_bit_values(16))
    [0, 0, 0, 0, 1]
    """
    digits = bin(number)[2:]
    return map(int, reversed(digits))


# from more_itertools
def padded(iterable, fillvalue=None, n=None, next_multiple=False):
    """Yield the elements from *iterable*, followed by *fillvalue*, such that
    at least *n* items are emitted.

        >>> list(padded([1, 2, 3], '?', 5))
        [1, 2, 3, '?', '?']

    If *next_multiple* is ``True``, *fillvalue* will be emitted until the
    number of items emitted is a multiple of *n*::

        >>> list(padded([1, 2, 3, 4], n=3, next_multiple=True))
        [1, 2, 3, 4, None, None]

    If *n* is ``None``, *fillvalue* will be emitted indefinitely.

    """
    it = iter(iterable)
    if n is None:
        yield from itertools.chain(it, itertools.repeat(fillvalue))
    elif n < 1:
        raise ValueError('n must be at least 1')
    else:
        item_count = 0
        for item in it:
            yield item
            item_count += 1

        remaining = (n - item_count) % n if next_multiple else n - item_count
        for _ in range(remaining):
            yield fillvalue


def compound(mode):
    """
    Support multiple, comma-separated Unix chmod symbolic modes.

    >>> oct(compound('a=r,u+w')(0))
    '0o644'
    """
    return compose(*map(simple, reversed(mode.split(','))))


def simple(mode):
    """
    Convert a Unix chmod symbolic mode like ``'ugo+rwx'`` to a function
    suitable for applying to a mask to affect that change.

    >>> mask = simple('ugo+rwx')
    >>> mask(0o554) == 0o777
    True

    >>> simple('go-x')(0o777) == 0o766
    True

    >>> simple('o-x')(0o445) == 0o444
    True

    >>> simple('a+x')(0) == 0o111
    True

    >>> simple('a=rw')(0o057) == 0o666
    True

    >>> simple('u=x')(0o666) == 0o166
    True

    >>> simple('g=')(0o157) == 0o107
    True

    >>> simple('gobbledeegook')
    Traceback (most recent call last):
    ValueError: ('Unrecognized symbolic mode', 'gobbledeegook')
    """
    # parse the symbolic mode
    parsed = re.match('(?P<who>[ugoa]+)(?P<op>[-+=])(?P<what>[rwx]*)$', mode)
    if not parsed:
        raise ValueError("Unrecognized symbolic mode", mode)

    # generate a mask representing the specified permission
    spec_map = dict(r=4, w=2, x=1)
    specs = (spec_map[perm] for perm in parsed.group('what'))
    spec = functools.reduce(operator.or_, specs, 0)

    # now apply spec to each subject in who
    shift_map = dict(u=6, g=3, o=0)
    who = parsed.group('who').replace('a', 'ugo')
    masks = (spec << shift_map[subj] for subj in who)
    mask = functools.reduce(operator.or_, masks)

    op = parsed.group('op')

    # if op is -, invert the mask
    if op == '-':
        mask ^= 0o777

    # if op is =, retain extant values for unreferenced subjects
    if op == '=':
        masks = (0o7 << shift_map[subj] for subj in who)
        retain = functools.reduce(operator.or_, masks) ^ 0o777

    op_map = {
        '+': operator.or_,
        '-': operator.and_,
        '=': lambda mask, target: target & retain ^ mask,
    }
    return functools.partial(op_map[op], mask)


class Permissions(int):
    """
    >>> perms = Permissions(0o764)
    >>> oct(perms)
    '0o764'
    >>> perms.symbolic
    'rwxrw-r--'
    >>> str(perms)
    'rwxrw-r--'
    >>> str(Permissions(0o222))
    '-w--w--w-'
    """

    @property
    def symbolic(self):
        return ''.join(
            ['-', val][bit] for val, bit in zip(itertools.cycle('rwx'), self.bits)
        )

    @property
    def bits(self):
        return reversed(tuple(padded(gen_bit_values(self), 0, n=9)))

    def __str__(self):
        return self.symbolic
