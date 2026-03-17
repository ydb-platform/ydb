__all__ = ['isnone', 'notnone', 'inc', 'dec', 'even', 'odd']


class EmptyType:
    def __repr__(self):
        return 'EMPTY'

EMPTY = EmptyType() # Used as unique default for optional arguments


def isnone(x):
    return x is None

def notnone(x):
    return x is not None


def inc(x):
    return x + 1

def dec(x):
    return x - 1

def even(x):
    return x % 2 == 0

def odd(x):
    return x % 2 == 1
