LIBRARY()

SRCS(
    dq_pq_meta_extractor.cpp
    dq_pq_rd_read_actor.cpp
    dq_pq_read_actor.cpp
    dq_pq_read_actor_base.cpp
    dq_pq_write_actor.cpp
    probes.cpp
)

PEERDIR(
    ydb/core/fq/libs/graph_params/proto
    ydb/core/fq/libs/protos
    ydb/core/fq/libs/row_dispatcher
    ydb/library/actors/log_backend
    ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto
    yql/essentials/public/types
    yql/essentials/utils/log
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
