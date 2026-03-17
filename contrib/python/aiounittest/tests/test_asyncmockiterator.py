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
