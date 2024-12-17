"""Provides compatibility with different pytest versions."""

from inspect import signature

__GETFIXTUREDEFS_2ND_PAR_IS_STR = None


def getfixturedefs(fixturemanager, name, item):
    """Calls FixtureManager.getfixturedefs in a way compatible with Python
    versions before and after the change described in pytest-dev/pytest#11785.
    """
    getfixturedefs = fixturemanager.getfixturedefs
    itemarg = __resolve_getfixturedefs_2nd_arg(getfixturedefs, item)
    return getfixturedefs(name, itemarg)


def __resolve_getfixturedefs_2nd_arg(getfixturedefs, item):
    # Starting from pytest 8.1, getfixturedefs requires the item itself.
    # In earlier versions it requires the nodeid string.
    return item.nodeid if __2nd_parameter_is_str(getfixturedefs) else item


def __2nd_parameter_is_str(getfixturedefs):
    global __GETFIXTUREDEFS_2ND_PAR_IS_STR
    if __GETFIXTUREDEFS_2ND_PAR_IS_STR is None:
        __GETFIXTUREDEFS_2ND_PAR_IS_STR =\
            __get_2nd_parameter_type(getfixturedefs) is str
    return __GETFIXTUREDEFS_2ND_PAR_IS_STR


def __get_2nd_parameter_type(fn):
    return list(
        signature(fn).parameters.values()
    )[1].annotation
