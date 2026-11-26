YQL_LIBRARY()

SRCS(
    processor_scan.h
    scan_actor_base_impl.h
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/mind
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/sys_view/service
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/protos
    yql/essentials/public/issue
)

END()
