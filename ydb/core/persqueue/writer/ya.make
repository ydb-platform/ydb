YQL_LIBRARY()

SRCS(
    metadata_initializers.cpp
    partition_chooser.cpp  # gn: partition_chooser
    partition_chooser_impl.cpp
    source_id_encoding.cpp
    writer.cpp
)
# gn: partition_chooser headers partition_chooser.h partition_chooser_impl.h

PEERDIR(
    ydb/library/actors/core
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/grpc_services/cancelation/protos
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/library/wilson_ids
    ydb/public/lib/base
    ydb/public/lib/deprecated/kicli
    ydb/public/sdk/cpp/src/client/params
    ydb/services/metadata/initializer
)
# gn: partition_chooser peerdir ydb/core/persqueue/public:utils

END()

RECURSE_FOR_TESTS(
    ut
)
