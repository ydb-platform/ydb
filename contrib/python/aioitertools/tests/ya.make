PY3TEST()

PEERDIR(
    contrib/python/aioitertools
)

SRCDIR(contrib/python/aioitertools/aioitertools/tests)

TEST_SRCS(
    asyncio.py
    builtins.py
    helpers.py
    itertools.py
    more_itertools.py
)

NO_LINT()

END()
