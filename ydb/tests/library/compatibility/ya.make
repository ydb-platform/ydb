RECURSE(binaries)
RECURSE(configs)

PY23_LIBRARY()

PEERDIR(
    ydb/tests/library/fixtures
)

PY_SRCS(
    fixtures.py
)

END()
