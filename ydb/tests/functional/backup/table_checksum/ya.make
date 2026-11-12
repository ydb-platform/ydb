PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

TEST_SRCS(
    conftest.py
    test_table_checksum.py
)

PEERDIR(
    contrib/python/boto3
    contrib/python/grpcio
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
SPLIT_FACTOR(2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16 cpu:4)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
