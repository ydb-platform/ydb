UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SPLIT_FACTOR(60)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(300)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    sdk_sessions_pool_ut.cpp
)

PEERDIR(
    library/cpp/grpc/client
    ydb/core/testlib/default
    ydb/core/testlib
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:14)

END()
