G_BENCHMARK(schemeshard_ut_copy_table_bench)

SRCS(
    b_copy_table.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/pg
    ydb/core/tx
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/api/protos
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()