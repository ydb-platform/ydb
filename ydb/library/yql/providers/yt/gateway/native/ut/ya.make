UNITTEST()

SRCS(
    yql_yt_native_folders_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/core/ut_common
    library/cpp/testing/mock_server
    library/cpp/testing/common
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
