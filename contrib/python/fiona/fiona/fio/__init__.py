"""Fiona's command line interface"""

from functools import wraps


def with_context_env(f):
    """Pops the Fiona Env from the passed context and executes the
    wrapped func in the context of that obj.

    Click's pass_context decorator must precede this decorator, or else
    there will be no context in the wrapper args.
    """
    @wraps(f)
    def wrapper(*args, **kwds):
        ctx = args[0]
        env = ctx.obj.pop('env')
        with env:
            return f(*args, **kwds)
    return wrapper
