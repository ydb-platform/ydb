UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(7)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_table_split_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/client
    ydb/core/formats/arrow
    ydb/core/testlib
    ydb/core/testlib/default
    ydb/core/tx/datashard
    ydb/library/formats/arrow/switch
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
