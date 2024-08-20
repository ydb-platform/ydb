UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(7)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:14)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(300)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_table_split_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/getopt
    ydb/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/grpc_services/base
    ydb/core/testlib
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/jsonpath
    ydb/public/lib/experimental
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_coordination
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_extension
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_monitoring
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
