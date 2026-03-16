PY3TEST()

PEERDIR(
    contrib/python/webauthn
)

NO_LINT()

ALL_PYTEST_SRCS(RECURSIVE)

END()
