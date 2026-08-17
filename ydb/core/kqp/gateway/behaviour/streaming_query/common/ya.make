LIBRARY()

SRCS(
    utils.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/yql/providers/pq/proto
    ydb/library/yverify_stream
    yql/essentials/sql/v1/translation
)

YQL_LAST_ABI_VERSION()

END()
