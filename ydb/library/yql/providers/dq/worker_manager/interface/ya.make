LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/library/yql/utils/log
    ydb/library/yql/dq/common
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
)

YQL_LAST_ABI_VERSION()

SET(
    SOURCE
    events.cpp
    worker_info.cpp
    counters.cpp
)

SRCS(
    ${SOURCE}
)

END()
