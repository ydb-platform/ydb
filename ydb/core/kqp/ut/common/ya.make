LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_ut_common.cpp
    kqp_ut_common.h
)

PEERDIR(
    ydb/core/testlib
    ydb/library/yql/public/udf
    ydb/library/yql/utils/backtrace
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()
 
END()
