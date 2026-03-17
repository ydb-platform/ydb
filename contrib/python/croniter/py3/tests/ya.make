PY3TEST()

PEERDIR(
    contrib/python/croniter
)

SRCDIR(contrib/python/croniter/py3)

PY_SRCS(
    TOP_LEVEL
    croniter/tests/__init__.py
    croniter/tests/base.py
)

TEST_SRCS(
    croniter/tests/test_croniter.py
    croniter/tests/test_croniter_dst_repetition.py
    croniter/tests/test_croniter_hash.py
    croniter/tests/test_croniter_random.py
    croniter/tests/test_croniter_range.py
    croniter/tests/test_croniter_speed.py
)

NO_LINT()

END()
