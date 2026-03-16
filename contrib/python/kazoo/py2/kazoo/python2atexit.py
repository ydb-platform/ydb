"""Uses the old atexit with added unregister for python 2.x
and the new atexit for python 3.x
"""
import atexit
import sys


__all__ = ["register", "unregister"]


_exithandlers = []


def _run_exitfuncs():
    """run any registered exit functions

    _exithandlers is traversed in reverse order so functions are executed
    last in, first out.
    """

    exc_info = None
    while _exithandlers:
        func, targs, kargs = _exithandlers.pop()
        try:
            func(*targs, **kargs)
        except SystemExit:
            exc_info = sys.exc_info()
        except:
            import traceback
            sys.stderr.write("Error in atexit._run_exitfuncs:\n")
            traceback.print_exc()
            exc_info = sys.exc_info()

    if exc_info is not None:
        raise exc_info[0](exc_info[1])


def register(func, *targs, **kargs):
    """register a function to be executed upon normal program termination

    func - function to be called at exit
    targs - optional arguments to pass to func
    kargs - optional keyword arguments to pass to func

    func is returned to facilitate usage as a decorator.
    """
    if hasattr(atexit, "unregister"):
        atexit.register(func, *targs, **kargs)
    else:
        _exithandlers.append((func, targs, kargs))
    return func


def unregister(func):
    """remove func from the list of functions that are registered
    doesn't do anything if func is not found

    func = function to be unregistered
    """
    if hasattr(atexit, "unregister"):
        atexit.unregister(func)
    else:
        handler_entries = [e for e in _exithandlers if e[0] == func]
        for e in handler_entries:
            _exithandlers.remove(e)

if not hasattr(atexit, "unregister"):
    # Only in python 2.x
    atexit.register(_run_exitfuncs)
