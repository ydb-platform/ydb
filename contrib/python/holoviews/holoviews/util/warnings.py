import inspect
import os
import sys
import warnings

import param
from packaging.version import Version

__all__ = (
    "HoloviewsDeprecationWarning",
    "HoloviewsUserWarning",
    "deprecated",
    "find_stack_level",
    "warn",
)


def warn(message, category=None, stacklevel=None):
    if stacklevel is None:
        stacklevel = find_stack_level()

    warnings.warn(message, category, stacklevel=stacklevel)


def find_stack_level():
    """Find the first place in the stack that is not inside Holoviews and Param.
    Inspired by: pandas.util._exceptions.find_stack_level

    """
    import pyviz_comms

    import holoviews as hv

    pkg_dir = os.path.dirname(hv.__file__)
    test_dir = os.path.join(pkg_dir, "tests")

    ignore_paths = (
        pkg_dir,
        os.path.dirname(param.__file__),
        os.path.dirname(pyviz_comms.__file__),
    )

    if ipc := sys.modules.get("IPython.core"):
        ignore_paths = (*ignore_paths, os.path.dirname(ipc.__file__))

    frame = inspect.currentframe()
    try:
        stacklevel = 0
        while frame:
            fname = inspect.getfile(frame)
            if fname.startswith(ignore_paths) and not fname.startswith(test_dir):
                frame = frame.f_back
                stacklevel += 1
            else:
                break
    finally:
        # See: https://docs.python.org/3/library/inspect.html#inspect.Traceback
        del frame

    return stacklevel


def deprecated(remove_version, old, new=None, extra=None, *, repr_old=True, repr_new=True):
    import holoviews as hv

    current_version = Version(Version(hv.__version__).base_version)

    if isinstance(remove_version, str):
        remove_version = Version(remove_version)

    if remove_version <= current_version:
        # This error is mainly for developers to remove the deprecated.
        raise ValueError(
            f"{old!r} should have been removed in {remove_version}, current version {current_version}."
        )

    old_str = repr(old) if repr_old else str(old)
    message = f"{old_str} is deprecated and will be removed in version {remove_version}."

    if new:
        new_str = repr(new) if repr_new else str(new)
        message = f"{message[:-1]}, use {new_str} instead."

    if extra:
        message += " " + extra.strip()

    warn(message, HoloviewsDeprecationWarning)


class HoloviewsDeprecationWarning(DeprecationWarning):
    """A Holoviews-specific ``DeprecationWarning`` subclass.
    Used to selectively filter Holoviews deprecations for unconditional display.

    """


class HoloviewsUserWarning(UserWarning):
    """A Holoviews-specific ``UserWarning`` subclass.
    Used to selectively filter Holoviews warnings for unconditional display.

    """
