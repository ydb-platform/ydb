UNITTEST_FOR(ydb/core/grpc_services)

FORK_SUBTESTS()

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    rpc_calls_ut.cpp
    rpc_cms_ut.cpp
    rpc_bridge_ut.cpp
    rpc_load_rows_ut.cpp
    operation_helpers_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/client/scheme_cache_lib
    ydb/core/formats/arrow
    ydb/core/scheme
    ydb/core/testlib/default
    ydb/core/kqp/ut/common
    ydb/core/tx/datashard/ut_common
    ydb/core/persqueue/writer
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/cms
    library/cpp/json
)

END()
