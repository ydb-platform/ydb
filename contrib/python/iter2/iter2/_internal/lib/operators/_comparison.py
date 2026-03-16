from __future__ import annotations

from ..undefined import UNDEFINED


# ---

def equals(obj1, obj2=UNDEFINED):
    if UNDEFINED.is_not(obj2):
        return obj1 == obj2
    else:
        return obj1.__eq__


def greater(obj1, obj2=UNDEFINED):
    if UNDEFINED.is_not(obj2):
        return obj1 > obj2
    else:
        return (lambda obj2: obj2 > obj1)


def greater_or_equals(obj1, obj2=UNDEFINED):
    if UNDEFINED.is_not(obj2):
        return obj1 >= obj2
    else:
        return (lambda obj2: obj2 >= obj1)


def lesser(obj1, obj2=UNDEFINED):
    if UNDEFINED.is_not(obj2):
        return obj1 < obj2
    else:
        return (lambda obj2: obj2 < obj1)


def lesser_or_equals(obj1, obj2=UNDEFINED):
    if UNDEFINED.is_not(obj2):
        return obj1 <= obj2
    else:
        return (lambda obj2: obj2 <= obj1)
