LIBRARY()

SRCS(
    kqp_run_script_actor.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/executer_actor
    ydb/core/kqp/proxy_service/proto
    ydb/core/protos
    ydb/library/actors/core
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
