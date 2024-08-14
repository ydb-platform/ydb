PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    library/python/testing/yatest_common
)

TEST_SRCS(
    test_unknown_data_source.py
)

PY_SRCS(
    conftest.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
