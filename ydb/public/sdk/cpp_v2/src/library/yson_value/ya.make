LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    ydb_yson_value.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/public/sdk/cpp_v2/src/client/result
    ydb/public/sdk/cpp_v2/src/client/value
    ydb/public/sdk/cpp_v2/src/library/uuid
)

END()
