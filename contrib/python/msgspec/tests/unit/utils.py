import inspect
import sys
import textwrap
import types
import uuid
from contextlib import contextmanager


@contextmanager
def temp_module(code):
    """Mutually recursive struct types defined inside functions don't work (and
    probably never will). To avoid populating a bunch of test structs in the
    top level of this module, we instead create a temporary module per test to
    exec whatever is needed for that test"""
    code = textwrap.dedent(code)
    name = f"temp_{uuid.uuid4().hex}"
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    try:
        exec(code, mod.__dict__)
        yield mod
    finally:
        sys.modules.pop(name, None)


@contextmanager
def max_call_depth(n):
    cur_depth = len(inspect.stack(0))
    orig = sys.getrecursionlimit()
    try:
        # Our measure of the current stack depth can be off by a bit. Trying to
        # set a recursionlimit < the current depth will raise a RecursionError.
        # We just try again with a slightly higher limit, bailing after an
        # unreasonable amount of adjustments.
        for i in range(64):
            try:
                sys.setrecursionlimit(cur_depth + i + n)
                break
            except RecursionError:
                pass
        else:
            raise ValueError(
                "Failed to set low recursion limit, something is wrong here"
            )
        yield
    finally:
        sys.setrecursionlimit(orig)


@contextmanager
def package_not_installed(name):
    try:
        orig = sys.modules.get(name)
        sys.modules[name] = None
        yield
    finally:
        if orig is not None:
            sys.modules[name] = orig
        else:
            del sys.modules[name]
