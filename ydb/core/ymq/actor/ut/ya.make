UNITTEST()

PEERDIR(
    contrib/libs/yaml-cpp
    ydb/core/grpc_services/local_rpc
    ydb/core/mind/address_classification
    ydb/core/testlib/audit_helpers
    ydb/core/testlib/default
    ydb/core/ymq/actor
    ydb/core/ymq/base
    ydb/core/ymq/http
    ydb/services/ymq
)

SRCS(
    audit_log_ut.cpp
    infly_ut.cpp
    message_delay_stats_ut.cpp
    sha256_ut.cpp
    metering_ut.cpp
    source_address_ut.cpp
    topic_sqs_action_metrics_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
