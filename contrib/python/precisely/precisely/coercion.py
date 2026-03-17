from .base import is_matcher
from .core_matchers import equal_to


def to_matcher(value):
    if is_matcher(value):
        return value
    else:
        return equal_to(value)
