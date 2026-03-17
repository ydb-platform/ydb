import sys
import traceback


def get_stacktrace(limit=None):
    """
    Get a full stacktrace for the current state of execution.

    Include the current state of the stack, minus this function.
    If there is an active exception, include the stacktrace information from
    the exception as well.

    :param int limit:
        Optionally limit stack trace size results. This parmaeters has the same
        meaning as the `limit` parameter in `traceback.print_stack`.
    :returns:
        List of stack trace objects, in the same form as
        `traceback.extract_stack`.
    """
    if limit is not None and limit == 0:
        # Nothing to return. This is consistent with the behavior of the
        # functions in the `traceback` module.
        return []

    stack = traceback.extract_stack()
    # Remove this `get_stacktrace()` function call from the stack info.
    # For what we want to report, this is superfluous information and arguably
    # adds garbage to the report.
    # Also drop the `traceback.extract_stack()` call above from the returned
    # stack info, since this is also superfluous.
    stack = stack[:-2]

    _exc_type, _exc, exc_traceback = sys.exc_info()
    if exc_traceback is not None:
        # If and only if there is a currently triggered exception, combine the
        # exception traceback information with the current stack state to get a
        # complete trace.
        exc_stack = traceback.extract_tb(exc_traceback)
        stack += exc_stack

    # Limit the stack trace size, if a limit was specified:
    if limit is not None:
        # Copy the behavior of `traceback` functions with a `limit` argument.
        # See https://docs.python.org/3/library/traceback.html.
        if limit > 0:
            # limit > 0: include the last `limit` items
            stack = stack[-limit:]
        else:
            # limit < 0: include the first `abs(limit)` items
            stack = stack[:abs(limit)]
    return stack
