from aiounittest import futurized, AsyncTestCase
from unittest.mock import Mock, patch

import dummy_math

class MyAddTest(AsyncTestCase):

    async def test_add(self):
        mock_sleep = Mock(return_value=futurized('whatever'))
        patch('dummy_math.sleep', mock_sleep).start()
        ret = await dummy_math.add(5, 6)
        self.assertEqual(ret, 11)
        mock_sleep.assert_called_once_with(666)

    async def test_fail(self):
        mock_sleep = Mock(return_value=futurized(Exception('whatever')))
        patch('dummy_math.sleep', mock_sleep).start()
        with self.assertRaises(Exception) as e:
            await dummy_math.add(5, 6)
        mock_sleep.assert_called_once_with(666)
