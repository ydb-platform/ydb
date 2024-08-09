LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    ydb_json_value.cpp
)

PEERDIR(
    library/cpp/json/writer
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp_v2/src/client/result
    ydb/public/sdk/cpp_v2/src/client/value
    ydb/public/sdk/cpp_v2/src/library/uuid
)

END()
