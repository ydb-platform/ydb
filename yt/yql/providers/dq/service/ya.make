LIBRARY()

SRCS(
    grpc_service.cpp
    grpc_session.cpp
    service_node.cpp
    interconnect_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/dnsresolver
    contrib/ydb/library/actors/interconnect
    library/cpp/build_info
    contrib/ydb/library/grpc/server
    contrib/ydb/library/grpc/server/actors
    library/cpp/svnversion
    library/cpp/threading/future
    yql/essentials/sql
    contrib/ydb/public/api/protos
    yql/essentials/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/interface
    contrib/ydb/library/yql/providers/dq/worker_manager
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    yt/yql/providers/dq/config
)

YQL_LAST_ABI_VERSION()

END()
