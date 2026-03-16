PY3TEST()

NO_LINT()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/attrs
    contrib/python/msgspec
    contrib/python/PyYAML
    contrib/python/tomli
    contrib/python/typing-extensions
)

ALL_PYTEST_SRCS(RECURSIVE)

END()
