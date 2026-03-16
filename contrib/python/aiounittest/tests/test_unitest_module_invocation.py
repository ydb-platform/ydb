import sys
from unittest.main import main


def test_specific_test():
    sys.argv = ['TEST']
    sys.argv.append('test_asynctestcase.TestAsyncCase.test_await_async_add')
    a = main(module=None, exit=False)
