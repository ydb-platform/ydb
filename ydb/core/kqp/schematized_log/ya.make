LIBRARY()

SRCS(
    base_schematized_log_writer.cpp
    column_shard_log_writer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/proto/opentelemetry
    library/cpp/lwtrace/protos
    library/cpp/messagebus/monitoring
    ydb/core/base/generated
    ydb/core/control/lib
    ydb/core/grpc_services/cancelation/protos
    ydb/core/protos
    ydb/library/aclib/protos/acl
    ydb/library/aclib/protos/identity
    ydb/library/actors/struct_log
)

YQL_LAST_ABI_VERSION()

END()
