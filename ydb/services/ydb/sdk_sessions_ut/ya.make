UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SPLIT_FACTOR(60)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    sdk_sessions_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/core/testlib/default
    ydb/core/testlib
    ydb/public/sdk/cpp/src/client/table
    ydb/public/lib/ut_helpers
)

YQL_LAST_ABI_VERSION()

END()
