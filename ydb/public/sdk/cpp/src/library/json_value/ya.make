LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    ydb_json_value.cpp
)

PEERDIR(
    library/cpp/json/writer
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/library/uuid
)

END()
