PY3TEST()

WITHOUT_LICENSE_TEXTS()

FORK_SUBTESTS()

PEERDIR(
    contrib/python/rapidfuzz
    contrib/python/hypothesis
    contrib/python/pandas
)

SIZE(MEDIUM)

PY_SRCS(
    NAMESPACE tests
    __init__.py
    common.py
    distance/__init__.py
    distance/common.py
)

ALL_PYTEST_SRCS(
    RECURSIVE
    ONLY_TEST_FILES
)

NO_LINT()

END()
