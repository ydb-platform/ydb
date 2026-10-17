YQL_LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    yql/essentials/utils/log
    ydb/library/yql/dq/common
    yql/essentials/providers/common/metrics
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
)

SRCS(
    events.cpp
    worker_info.cpp
    counters.cpp
)
# gn: peerdir ydb/library/yql/providers/dq/common:attrs

END()
