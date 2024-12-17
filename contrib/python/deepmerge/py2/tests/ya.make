PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/deepmerge
)

SRCDIR(
    contrib/python/deepmerge/py2/deepmerge/tests
)

TEST_SRCS(
    __init__.py
    strategy/__init__.py
    strategy/test_core.py
    strategy/test_set_merge.py
    strategy/test_type_conflict.py
    test_full.py
    test_merger.py
)

NO_LINT()

END()
