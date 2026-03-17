from collections.abc import Iterable


def str_coercible(cls):
    def __str__(self):
        return self.__unicode__()

    cls.__str__ = __str__
    return cls


def is_sequence(value):
    return (
        isinstance(value, Iterable) and not isinstance(value, str)
    )


def starts_with(iterable, prefix):
    """
    Returns whether or not given iterable starts with given prefix.
    """
    return list(iterable)[0:len(prefix)] == list(prefix)
