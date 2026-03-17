import functools

from flex.utils import (
    is_non_string_iterable,
    is_value_of_any_type,
)
from flex.constants import EMPTY


def partial_safe_wraps(wrapped_func, *args, **kwargs):
    """
    A version of `functools.wraps` that is safe to wrap a partial in.
    """
    if isinstance(wrapped_func, functools.partial):
        return partial_safe_wraps(wrapped_func.func)
    else:
        return functools.wraps(wrapped_func)


def maybe_iterable(func):
    @partial_safe_wraps(func)
    def inner(value):
        if is_non_string_iterable(value):
            # TODO: this should collect all errors that are raised and re-raise
            # them as a list.
            return list(map(func, value))
        else:
            return func(value)
    return inner


def skip_if_not_of_type(*types):
    def outer(func):
        @partial_safe_wraps(func)
        def inner(value, *args, **kwargs):
            if value is EMPTY or is_value_of_any_type(value, types):
                return func(value, *args, **kwargs)
        return inner
    return outer


def skip_if_empty(func):
    """
    Decorator for validation functions which makes them pass if the value
    passed in is the EMPTY sentinal value.
    """
    @partial_safe_wraps(func)
    def inner(value, *args, **kwargs):
        if value is EMPTY:
            return
        else:
            return func(value, *args, **kwargs)
    return inner


def skip_if_any_kwargs_empty(*kwargs):
    def outer(func):
        @partial_safe_wraps(func)
        def inner(*args, **inner_kwargs):
            if any(inner_kwargs.get(key) is EMPTY for key in kwargs):
                return
            return func(*args, **inner_kwargs)
        return inner
    return outer


RESERVED_WORDS = (
    'in',
    'format',
    'type',
)


def rewrite_reserved_words(func):
    """
    Given a function whos kwargs need to contain a reserved word such as `in`,
    allow calling that function with the keyword as `in_`, such that function
    kwargs are rewritten to use the reserved word.
    """
    @partial_safe_wraps(func)
    def inner(*args, **kwargs):
        for word in RESERVED_WORDS:
            key = "{0}_".format(word)
            if key in kwargs:
                kwargs[word] = kwargs.pop(key)
        return func(*args, **kwargs)
    return inner


def suffix_reserved_words(func):
    """
    Given a function that is called with a reseved word, rewrite the keyword
    with an underscore suffix.
    """
    @partial_safe_wraps(func)
    def inner(*args, **kwargs):
        for word in RESERVED_WORDS:
            if word in kwargs:
                key = "{0}_".format(word)
                kwargs[key] = kwargs.pop(word)
        return func(*args, **kwargs)
    return inner


def pull_keys_from_obj(*keys):
    def outer(func):
        @skip_if_empty
        @partial_safe_wraps(func)
        def inner(obj, *args, **kwargs):
            for key in keys:
                assert key not in kwargs
                kwargs[key] = obj.get(key, EMPTY)
            if all((v is EMPTY for v in kwargs.values())):
                return
            return func(**kwargs)
        return inner
    return outer
