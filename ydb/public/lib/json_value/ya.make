LIBRARY()

SRCS(
    ydb_json_value.cpp
    ydb_json_value_ut.cpp
)

PEERDIR(
    library/cpp/json/writer
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/types/uuid
)

END()

RECURSE_FOR_TESTS(
    ut
)
