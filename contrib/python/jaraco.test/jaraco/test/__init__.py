from __future__ import annotations

import sys


def property_error(name: str) -> str:
    """
    Generate a regular expression for capturing the expected
    error messages that can result from attempting to set
    on a property without a setter.

    >>> class Foo:
    ...     @property
    ...     def bar(self):
    ...         return 'bar'
    >>> import pytest
    >>> with pytest.raises(AttributeError, match=property_error('Foo.bar')):
    ...     Foo().bar = 'anything'
    """
    class_, property_ = name.split('.')
    return (
        "can't set attribute"
        if sys.version_info < (3, 10)
        else f"can't set attribute {property_!r}"
        if sys.version_info < (3, 11)
        else f"property {property_!r} of {class_!r} object has no setter"
    )
