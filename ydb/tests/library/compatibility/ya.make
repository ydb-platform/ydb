RECURSE(binaries)
RECURSE(configs)

PY23_LIBRARY()

PEERDIR(
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
)

PY_SRCS(
    driver_seeds.py
    fixtures.py
)

END()

RECURSE_FOR_TESTS(
    ut
)
