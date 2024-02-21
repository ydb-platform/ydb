LIBRARY()

SRCS(
    dq_pq_meta_extractor.cpp
    dq_pq_read_actor.cpp
    dq_pq_write_actor.cpp
    probes.cpp
)

PEERDIR(
    ydb/library/actors/log_backend
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/public/types
    ydb/library/yql/utils/log
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

YQL_LAST_ABI_VERSION()

END()
