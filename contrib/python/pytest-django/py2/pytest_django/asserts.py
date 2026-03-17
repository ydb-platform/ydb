"""
Dynamically load all Django assertion cases and expose them for importing.
"""
from functools import wraps
from django.test import (
    TestCase, SimpleTestCase,
    LiveServerTestCase, TransactionTestCase
)

test_case = TestCase('run')


def _wrapper(name):
    func = getattr(test_case, name)

    @wraps(func)
    def assertion_func(*args, **kwargs):
        return func(*args, **kwargs)

    return assertion_func


__all__ = []
assertions_names = set()
assertions_names.update(
    set(attr for attr in vars(TestCase) if attr.startswith('assert')),
    set(attr for attr in vars(SimpleTestCase) if attr.startswith('assert')),
    set(attr for attr in vars(LiveServerTestCase) if attr.startswith('assert')),
    set(attr for attr in vars(TransactionTestCase) if attr.startswith('assert')),
)

for assert_func in assertions_names:
    globals()[assert_func] = _wrapper(assert_func)
    __all__.append(assert_func)
