UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

SPLIT_FACTOR(5)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        cpu:4
        ram:32
    )
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_index_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/public/lib/idx_test
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
