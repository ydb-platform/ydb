PY3TEST()

PEERDIR(
    contrib/python/asyncclick
    contrib/python/click
    contrib/python/trio
    contrib/python/typing-extensions
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
