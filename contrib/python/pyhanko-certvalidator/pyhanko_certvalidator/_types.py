# coding: utf-8

import inspect


def type_name(value):
    """
    Returns a user-readable name for the type of an object

    :param value:
        A value to get the type name of

    :return:
        A unicode string of the object's type name
    """

    if inspect.isclass(value):
        cls = value
    else:
        cls = value.__class__
    if cls.__module__ in {'builtins', '__builtin__'}:
        return cls.__name__
    return '%s.%s' % (cls.__module__, cls.__name__)
