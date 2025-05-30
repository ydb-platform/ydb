UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(7)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_table_split_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/getopt
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/grpc_services/base
    ydb/core/testlib
    yql/essentials/minikql/dom
    yql/essentials/minikql/jsonpath
    ydb/public/lib/experimental
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/extension_common
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/monitoring
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
