"""Various utility functions."""

from inspect import getframeinfo
from inspect import signature as _signature
from sys import _getframe

CONFIG_STACK = []


def get_args(func):
    """Get a list of argument names for a function.

    :param func: The function to inspect.

    :return: A list of argument names.
    :rtype: list
    """
    params = _signature(func).parameters.values()
    return [param.name for param in params if param.kind == param.POSITIONAL_OR_KEYWORD]


def get_parametrize_markers_args(node):
    return tuple(arg for mark in node.iter_markers("parametrize") for arg in mark.args)


def get_caller_module_locals(depth=2):
    """Get the caller module locals dictionary.

    We use sys._getframe instead of inspect.stack(0) because the latter is way slower, since it iterates over
    all the frames in the stack.
    """
    return _getframe(depth).f_locals


def get_caller_module_path(depth=2):
    """Get the caller module path.

    We use sys._getframe instead of inspect.stack(0) because the latter is way slower, since it iterates over
    all the frames in the stack.
    """
    frame = _getframe(depth)
    return getframeinfo(frame, context=0).filename
