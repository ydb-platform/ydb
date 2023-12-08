LIBRARY()

SRCS(
    metadata_initializers.cpp
    partition_chooser_impl.cpp
    source_id_encoding.cpp
    writer.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/grpc_services/cancelation/protos
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/public/lib/base
    ydb/public/lib/deprecated/kicli
    ydb/public/sdk/cpp/client/ydb_params
)

END()
