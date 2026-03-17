import inspect
from typing import (
    Union,
    Any,
    Iterable,
    TypeVar,
)

try:
    from typing import GenericMeta
except ImportError:
    from typing import _GenericAlias as GenericMeta

T = TypeVar("T")


class UnderscoreType:
    def __repr__(self):
        return '_'


class HeadType:
    def __repr__(self):
        return 'HEAD'


class TailType:
    def __repr__(self):
        return 'TAIL'


class PaddedValueType:
    def __repr__(self):
        return 'PaddedValue'


class NoDefaultType:
    pass


NoDefault = NoDefaultType()

PaddedValue = PaddedValueType()


class BoxedArgs:
    def __init__(self, obj):
        self.obj = obj

    def get(self):
        return self.obj


def pairwise(l):
    i = 0
    while i < len(l):
        yield l[i], l[i + 1]
        i += 2


def get_lambda_args_error_msg(action, var, err):
    try:
        code = inspect.getsource(action)
        return "Error passing arguments %s here:\n%s\n%s" % (var, code, err)
    except OSError:
        return "Error passing arguments %s:\n%s" % (var, err)


def is_dataclass(value):
    """
    Dataclass support is only enabled in Python 3.7+, or in 3.6 with the `dataclasses` backport installed
    """
    try:
        from dataclasses import is_dataclass
        return is_dataclass(value)
    except ImportError:
        return False


def get_extra(pattern):
    return getattr(pattern, "__extra__", None) or getattr(pattern, "__origin__", None)


def peek(iter_: Iterable[T]) -> T:
    return next(iter(iter_))


def is_newtype(pattern):
    return inspect.isfunction(pattern) and hasattr(pattern, '__supertype__')


def is_generic(pattern):
    return isinstance(pattern, GenericMeta)


def is_union(pattern):
    return isinstance(pattern, Union.__class__) or getattr(pattern, "__origin__", None) == Union


def is_typing_stuff(pattern):
    return pattern == Any or is_generic(pattern) or is_union(pattern) or is_newtype(pattern)


def get_real_type(subtype):
    if is_newtype(subtype):
        return get_real_type(subtype.__supertype__)
    else:
        return subtype
