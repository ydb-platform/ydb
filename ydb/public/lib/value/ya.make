LIBRARY()

SRCS(
    value.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/string_utils/base64
    ydb/core/protos
    ydb/library/mkql_proto/protos
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/client/ydb_value
)

END()
