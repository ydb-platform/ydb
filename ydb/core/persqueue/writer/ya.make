LIBRARY()

SRCS(
    metadata_initializers.cpp
    source_id_encoding.cpp
    writer.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/public/lib/base
    ydb/public/lib/deprecated/kicli
    ydb/public/sdk/cpp/client/ydb_params
)

END()
