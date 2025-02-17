# -*- coding: utf-8 -*-
import asyncio
import sys

try:
    # as from Py3.8 unittest supports coroutines as test functions
    from unittest import IsolatedAsyncioTestCase, skipIf


    def fail_on(**kw):  # noqa
        def outer(fn):
            def inner(*args, **kwargs):
                return fn(*args, **kwargs)

            return inner

        return outer


except ImportError:
    # fallback to asynctest
    from asynctest import fail_on, skipIf
    from asynctest.case import TestCase as IsolatedAsyncioTestCase

IS_GTE_PY38 = sys.version_info >= (3, 8)


class AsyncTestCase(IsolatedAsyncioTestCase):
    """Asynchronous test case class that covers up differences in usage
    between unittest (starting from Python 3.8) and asynctest.

    `setup` and `teardown` is used to be called before each test case
    (note: that they are in lowercase)
    """

    async def setup(self):
        pass

    async def teardown(self):
        pass

    if IS_GTE_PY38:
        # from Python3.8
        async def asyncSetUp(self):
            self.loop = asyncio.get_event_loop()
            await self.setup()

        async def asyncTearDown(self):
            await self.teardown()
    else:
        # asynctest
        use_default_loop = False

        async def setUp(self) -> None:
            await self.setup()

        async def tearDown(self) -> None:
            await self.teardown()
