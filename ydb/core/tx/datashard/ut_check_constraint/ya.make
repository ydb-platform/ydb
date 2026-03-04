UNITTEST()

PEERDIR(
    ydb/core/tx/datashard/ut_common
    ydb/core/tx/datashard
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    yql/essentials/public/udf/service/exception_policy
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_scan_check_constraints.cpp
)

END()
