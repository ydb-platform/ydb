UNITTEST()

PEERDIR(
    ydb/core/tx/datashard/ut_common
    ydb/core/tx/datashard
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    yql/essentials/public/udf/service/exception_policy
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/result
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_truncate.cpp
)

END()
