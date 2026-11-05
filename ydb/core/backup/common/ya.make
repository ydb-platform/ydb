LIBRARY()

SRCS(
    checksum.cpp
    encryption.cpp
    feature_flags.cpp
    fields_wrappers.cpp
    metadata.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/json
    ydb/core/backup/common/proto
    ydb/core/base
    ydb/core/util
    ydb/public/api/protos
    ydb/library/yverify_stream
)

GENERATE_ENUM_SERIALIZATION(metadata.h)

END()
