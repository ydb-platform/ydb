LIBRARY()

SRCS(
    json2_udf.cpp
    kqp_ut_common.cpp
    kqp_ut_common.h
    re2_udf.cpp
    string_udf.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/library/yql/public/udf
    ydb/library/yql/utils/backtrace
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_query
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

END()
