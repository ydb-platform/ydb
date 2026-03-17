"""
This module provides utilities for importing modules and handling exceptions.

Classes:
    DummyError(Exception):
        A custom exception class used as a default for exception handling.

Functions:
    import_global(name, modules=None, exceptions=DummyError, locals_=None,
        globals_=None, level=-1):
        Imports the requested items into the global scope, with support for
        relative imports and custom exception handling.
"""

from . import types


class DummyError(Exception):
    """A custom exception class used as a default for exception handling."""


# Legacy alias for DummyError
DummyException = DummyError


def import_global(  # noqa: C901
    name: str,
    modules: types.Optional[types.List[str]] = None,
    exceptions: types.ExceptionsType = DummyError,
    locals_: types.OptionalScope = None,
    globals_: types.OptionalScope = None,
    level: int = -1,
) -> types.Any:  # sourcery skip: hoist-if-from-if
    """Import the requested items into the global scope.

    WARNING! this method _will_ overwrite your global scope
    If you have a variable named `path` and you call `import_global('sys')`
    it will be overwritten with `sys.path`

    Args:
        name (str): the name of the module to import, e.g. sys
        modules (str): the modules to import, use None for everything
        exceptions (Exception): the exception to catch, e.g. ImportError
        locals_: the `locals()` method (in case you need a different scope)
        globals_: the `globals()` method (in case you need a different scope)
        level (int): the level to import from, this can be used for
        relative imports
    """
    frame = None
    name_parts: types.List[str] = name.split('.')
    modules_set: types.Set[str] = set()
    try:
        # If locals_ or globals_ are not given, autodetect them by inspecting
        # the current stack
        if locals_ is None or globals_ is None:
            import inspect

            frame = inspect.stack()[1][0]

            if locals_ is None:
                locals_ = frame.f_locals

            if globals_ is None:
                globals_ = frame.f_globals

        try:
            # Relative imports are supported (from .spam import eggs)
            if not name_parts[0]:
                name_parts = name_parts[1:]
                level = 1

            # raise IOError((name, level))
            module = __import__(
                name=name_parts[0] or '.',
                globals=globals_,
                locals=locals_,
                fromlist=name_parts[1:],
                level=max(level, 0),
            )

            # Make sure we get the right part of a dotted import (i.e.
            # spam.eggs should return eggs, not spam)
            try:
                for attr in name_parts[1:]:
                    module = getattr(module, attr)
            except AttributeError as e:
                raise ImportError(
                    'No module named ' + '.'.join(name_parts)
                ) from e

            # If no list of modules is given, autodetect from either __all__
            # or a dir() of the module
            if not modules:
                modules_set = set(getattr(module, '__all__', dir(module)))
            else:
                modules_set = set(modules).intersection(dir(module))

            # Add all items in modules to the global scope
            for k in set(dir(module)).intersection(modules_set):
                if k and k[0] != '_':
                    globals_[k] = getattr(module, k)
        except exceptions as e:
            return e
    finally:
        # Clean up, just to be sure
        del (
            name,
            name_parts,
            modules,
            modules_set,
            exceptions,
            locals_,
            globals_,
            frame,
        )
