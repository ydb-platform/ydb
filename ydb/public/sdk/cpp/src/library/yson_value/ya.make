LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    ydb_yson_value.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/library/uuid
)

END()
