LIBRARY()

SRCS(
    source_id_encoding.cpp
    metadata_initializers.cpp
    writer.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/public/lib/base
)

END()
