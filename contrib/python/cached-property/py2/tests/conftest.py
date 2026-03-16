
import sys

# Whether "import asyncio" works
has_asyncio = sys.version_info[0] == 3 and sys.version_info[1] >= 4

# Whether the async and await keywords work
has_async_await = sys.version_info[0] == 3 and sys.version_info[1] >= 5

# Whether "from asyncio import coroutine" *fails*
version_info = sys.version_info
dropped_asyncio_coroutine = version_info[0] == 3 and version_info[1] >= 11


print("conftest.py", has_asyncio, has_async_await)


collect_ignore = []

if not has_asyncio or dropped_asyncio_coroutine:
    collect_ignore.append("tests/test_coroutine_cached_property.py")

if not has_async_await:
    collect_ignore.append("tests/test_async_cached_property.py")
