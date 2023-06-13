LIBRARY()

SRCS(
    ydb_json_value.cpp
    ydb_json_value_ut.cpp
)

PEERDIR(
    library/cpp/json/writer
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_value
    ydb/library/uuid
)

END()

RECURSE_FOR_TESTS(
    ut
)
