LIBRARY()

SRCS(
    files.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/lib/ydb_cli/dump/util
)

GENERATE_ENUM_SERIALIZATION(dump.h)

END()
