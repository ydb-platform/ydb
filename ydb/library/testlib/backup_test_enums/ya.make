LIBRARY()

SRCS(
    backup_test_enums.h
)

GENERATE_ENUM_SERIALIZATION(backup_test_enums.h)

PEERDIR(
    ydb/core/tx/datashard
)

END()
