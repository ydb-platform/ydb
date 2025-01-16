LIBRARY()

SRCS(
    json2_udf.cpp
    kqp_ut_common.cpp
    kqp_ut_common.h
    re2_udf.cpp
    string_udf.cpp
    columnshard.cpp
    datetime2_udf.cpp
)

PEERDIR(
    library/cpp/testing/common
    ydb/core/kqp/federated_query
    ydb/core/testlib
    ydb/library/yql/providers/s3/actors_factory
    yql/essentials/public/udf
    yql/essentials/udfs/common/string
    yql/essentials/utils/backtrace
    ydb/public/lib/yson_value
    ydb/core/tx/columnshard/test_helper
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_query
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

END()
