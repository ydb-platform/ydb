import asyncio
import unittest
from .helpers import async_test


class AsyncTestCase(unittest.TestCase):
    ''' AsyncTestCase allows to test asynchoronus function.


    The usage is the same as :code:`unittest.TestCase`. It works with other test frameworks
    and runners (eg. `pytest`, `nose`) as well.

    AsyncTestCase can run:
        - test of synchronous code (:code:`unittest.TestCase`)
        - test of asynchronous code, supports syntax with
          :code:`async`/:code:`await` (Python 3.5+) and
          :code:`asyncio.coroutine`/:code:`yield from` (Python 3.4)

    Code to test:

    .. code-block:: python

            import asyncio

            async def async_add(x, y, delay=0.1):
                await asyncio.sleep(delay)
                return x + y

            async def async_one():
                await async_nested_exc()

            async def async_nested_exc():
                await asyncio.sleep(0.1)
                raise Exception('Test')


    Tests:

    .. code-block:: python

            import aiounittest

            class MyTest(aiounittest.AsyncTestCase):

                async def test_await_async_add(self):
                    ret = await async_add(1, 5)
                    self.assertEqual(ret, 6)

                async def test_await_async_fail(self):
                    with self.assertRaises(Exception) as e:
                        await async_one()


    '''

    def get_event_loop(self):
        ''' Method provides an event loop for the test

        It is called before each test, by default :code:`aiounittest.AsyncTestCase` creates the brand new event
        loop everytime. After completion, the loop is closed and then recreated, set as default,
        leaving asyncio clean.

        .. note::

            In the most common cases you don't have to bother about this method, the default implementation is a receommended one.
            But if, for some reasons, you want to provide your own event loop just override it. Note that :code:`AsyncTestCase` won't close such a loop.

        .. code-block:: python

            class MyTest(aiounittest.AsyncTestCase):

                def get_event_loop(self):
                    self.my_loop = asyncio.get_event_loop()
                    return self.my_loop


        '''
        return None

    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if name.startswith('test_') and asyncio.iscoroutinefunction(attr):
            return async_test(attr, loop=self.get_event_loop())
        else:
            return attr
