import pytest
from functools import wraps

HAS_STASH = hasattr(pytest, 'StashKey')


def create_stashkey_safe():
    """
    If pytest stash is available, returns a new stash key.
    Otherwise, returns `None`.
    """

    return pytest.StashKey() if HAS_STASH else None


def stash_get_safe(item, key):
    """
    If pytest stash is available and contains the key, retrieves the associated value.
    Otherwise, returns `None`.
    """

    if HAS_STASH and key in item.stash:
        return item.stash[key]


def stash_set_safe(item: pytest.Item, key, value):
    """
    If pytest stash is available, associates the value with the key in the stash.
    Otherwise, does nothing.
    """

    if HAS_STASH:
        item.stash[key] = value


def stashed(arg=None):
    """
    Cashes the result of the decorated function in the pytest item stash.
    The first argument of the function must be a pytest item.

    In pytest<7.0 the stash is not available, so the decorator does nothing.
    """

    key = create_stashkey_safe() if arg is None or callable(arg) else arg

    def decorator(func):
        if not HAS_STASH:
            return func

        @wraps(func)
        def wrapper(item, *args, **kwargs):
            if key in item.stash:
                return item.stash[key]

            value = func(item, *args, **kwargs)
            item.stash[key] = value
            return value

        return wrapper

    return decorator(arg) if callable(arg) else decorator
