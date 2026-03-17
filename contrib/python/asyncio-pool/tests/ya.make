PY3TEST()
SIZE(SMALL)

PEERDIR(
    contrib/python/asyncio-pool
    contrib/python/async-timeout
    contrib/python/pytest-asyncio
)

TEST_SRCS(
    test_base.py
    test_callbacks.py
    test_map.py
    test_spawn.py
)

FORK_SUBTESTS()

NO_LINT()

END()
