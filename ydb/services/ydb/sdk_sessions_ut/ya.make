UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:14)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SPLIT_FACTOR(60)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(300)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    sdk_sessions_ut.cpp
)

PEERDIR(
    ydb/library/grpc/client
    ydb/core/testlib/default
    ydb/core/testlib
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
