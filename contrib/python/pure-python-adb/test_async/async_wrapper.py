import asyncio
import warnings



def _await(coro):
    with warnings.catch_warnings(record=True) as warns:
        ret = asyncio.get_event_loop().run_until_complete(coro)

        if warns:
            raise RuntimeError

        return ret


def awaiter(func):
    def sync_func(*args, **kwargs):
        return _await(func(*args, **kwargs))

    return sync_func
