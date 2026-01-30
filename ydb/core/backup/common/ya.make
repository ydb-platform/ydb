LIBRARY()

SRCS(
    checksum.cpp
    encryption.cpp
    metadata.cpp
    uploader_common.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/json
    ydb/core/backup/common/proto
    ydb/core/base
    ydb/core/util
    ydb/library/yverify_stream
)

GENERATE_ENUM_SERIALIZATION(metadata.h)

END()
