UNITTEST_FOR(ydb/core/grpc_services)

FORK_SUBTESTS()

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    rpc_cms_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/persqueue/writer
    ydb/core/testlib/default
    ydb/core/tx/datashard/ut_common
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/cms
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/public/udf/service/exception_policy
)

END()
