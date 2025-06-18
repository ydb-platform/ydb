LIBRARY()

SRCS(
    common.cpp
)

STYLE_CPP()

PEERDIR(
    ydb/core/kqp/rm_service
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/s3/actors_factory
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/query
)

YQL_LAST_ABI_VERSION()

END()
