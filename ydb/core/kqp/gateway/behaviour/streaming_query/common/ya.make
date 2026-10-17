YQL_LIBRARY()

SRCS(
    utils.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/conclusion
    ydb/library/yql/providers/pq/proto
    ydb/library/yverify_stream
    yql/essentials/minikql
    yql/essentials/sql/v1/translation
)

END()
