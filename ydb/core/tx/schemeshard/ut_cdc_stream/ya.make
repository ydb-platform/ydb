UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(2)

# Runs every op in this suite through the scheme change outbox and fails the
# test if any of them cannot resolve a target path.
ENV(YDB_SCHEME_CHANGE_CORPUS=1)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    library/cpp/json
)

SRCS(
    ut_cdc_stream.cpp
)

YQL_LAST_ABI_VERSION()

END()
