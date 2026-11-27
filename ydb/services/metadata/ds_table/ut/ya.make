UNITTEST_FOR(ydb/services/metadata/ds_table)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/basics
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_accessor_snapshot_base.cpp
)

END()
