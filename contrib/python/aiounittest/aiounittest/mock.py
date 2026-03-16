class AsyncMockIterator:
    ''' Allows to mock asynchronous for-loops.

    .. note::

        Supported only in Python 3.6 and newer, uses async/await syntax.


    .. code-block:: python

            from aiounittest import AsyncTestCase
            from aiounittest.mock import AsyncMockIterator
            from unittest.mock import Mock


            async def fetch_some_text(source):
                res = ''
                async for txt in source.paginate():
                    res += txt
                return res


            class MyAsyncMockIteratorTest(AsyncTestCase):

                async def test_add(self):
                    source = Mock()
                    mock_iter = AsyncMockIterator([
                        'asdf', 'qwer', 'zxcv'
                    ])
                    source.paginate.return_value = mock_iter

                    res = await fetch_some_text(source)

                    self.assertEqual(res, 'asdfqwerzxcv')
                    mock_iter.assertFullyConsumed()
                    mock_iter.assertIterCount(3)


    '''
    def __init__(self, seq):
        self.iter = iter(seq)
        self.__consumed = False
        self.__iter_count = 0

    def __aiter__(self, *args, **kwargs):
        return self

    async def __anext__(self, *args, **kwargs):
        try:
            val = next(self.iter)
            self.__iter_count += 1
            return val
        except StopIteration:
            self.__consumed = True
            raise StopAsyncIteration

    def assertFullyConsumed(self):
        ''' Whenever `async for` reached the end of the given sequence.
        '''
        assert self.__consumed, 'Iterator wasnt fully consumed'

    def assertIterCount(self, expected):
        ''' Checks whenever a number of a mock iteration matches expected.

        :param expected int: Expected number of iterations

        '''
        assert expected == self.__iter_count, '%d iterations instead of %d' % (self.__iter_count, expected)
