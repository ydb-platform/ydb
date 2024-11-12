LIBRARY()

SRCS(
    backup_restore_traits.cpp
)

GENERATE_ENUM_SERIALIZATION(backup_restore_traits.h)

PEERDIR(
    ydb/core/protos
    ydb/library/yverify_stream
)

END()
