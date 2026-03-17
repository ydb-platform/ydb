from functools import partial


def run_in_executor(executor, loop, func, *args, **kwargs):
    if kwargs:
        func = partial(func, **kwargs)

    return loop.run_in_executor(executor, func, *args)
