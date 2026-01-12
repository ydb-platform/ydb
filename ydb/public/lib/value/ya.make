LIBRARY()

SRCS(
    value.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/string_utils/base64
    ydb/core/protos
    yql/essentials/public/decimal
    ydb/library/mkql_proto/protos
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/src/client/value
)

END()

RECURSE_FOR_TESTS(
    ut
)
