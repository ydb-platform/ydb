import sys
import tornado

collect_ignore = []
if sys.version_info[:2] < (3, 5) or tornado.version_info[:2] < (4, 3):
    collect_ignore.append("test_async_await.py")


pytest_plugins = ["pytester"]
