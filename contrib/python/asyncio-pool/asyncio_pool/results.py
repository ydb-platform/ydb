from functools import partial


def result_noraise(future, flat=True):
    '''Extracts result from future, never raising an exception.

    If `flat` is True -- returns result or exception instance (including
    CancelledError), if `flat` is False -- returns tuple of (`result`,
    `exception` object).

    If traceback is needed -- just re-raise returned exception.'''
    try:
        res = future.result()
        return res if flat else (res, None)
    except BaseException as exc:
        return exc if flat else (None, exc)


class getres:
    dont = lambda fut: fut
    flat = partial(result_noraise, flat=True)
    pair = partial(result_noraise, flat=False)
