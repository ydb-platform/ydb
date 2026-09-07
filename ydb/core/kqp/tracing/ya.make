LIBRARY()

SRCS(
    kqp_execution_tracing.cpp
    kqp_scan_tracing.cpp
    kqp_query_tracing.cpp
)

PEERDIR(
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/library/actors/wilson
    ydb/library/wilson_ids
    ydb/library/yql/dq/actors/protos
    ydb/library/security
    ydb/public/api/protos
)

END()
