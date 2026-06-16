UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/grpc_services/local_rpc
    ydb/core/metering
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_set_column_constraint.cpp
)

END()
