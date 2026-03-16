import sys

from .results import getres
from .base_pool import BaseAioPool


if sys.version_info < (3, 6):  # this means 3.5  # TODO test 3.4?

    from .mx_asynciter import MxAsyncIterPool, iterwait

    class AioPool(MxAsyncIterPool, BaseAioPool): pass

else:
    from .mx_asyncgen import MxAsyncGenPool, iterwait

    class AioPool(MxAsyncGenPool, BaseAioPool): pass
