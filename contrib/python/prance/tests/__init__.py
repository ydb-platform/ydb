"""Test suite module for prance."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


def _find_imports(*args):
    """
    Helper sorting the named modules into existing and not existing.
    """
    import importlib

    exists = {
        True: [],
        False: [],
    }

    for name in args:
        name = name.replace("-", "_")
        try:
            importlib.import_module(name)
            exists[True].append(name)
        except ImportError:
            exists[False].append(name)

    return exists


def none_of(*args):
    """
    Return true if none of the named modules exist, false otherwise.
    """
    exists = _find_imports(*args)
    return len(exists[True]) == 0


def _platform(platform, *args):
    """
    Helper for platform()
    """
    ret = False
    for arg in args:
        if arg[0] == "!":
            arg = arg[1:]
            if platform != arg:
                ret = True
                break
        elif platform == arg:
            ret = True
            break

    return ret


def platform(*args):
    """
    Return true if the current platform is in one of the given ones.

    Platforms may be specified with leading '!', in which case they
    a match returns False.
    """
    import sys

    return _platform(sys.platform, *args)
