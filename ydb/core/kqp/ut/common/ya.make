LIBRARY()

SRCS(
    json2_udf.cpp
    kqp_ut_common.cpp
    kqp_ut_common.h
    re2_udf.cpp
    string_udf.cpp
    columnshard.cpp
    datetime2_udf.cpp
    unicode_udf.cpp
)

PEERDIR(
    library/cpp/testing/common
    ydb/core/kqp/federated_query
    ydb/core/testlib
    ydb/library/yql/providers/s3/actors_factory
    yql/essentials/public/udf
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/unicode_base
    yql/essentials/utils/backtrace
    ydb/public/lib/yson_value
    ydb/core/tx/columnshard/test_helper
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()
