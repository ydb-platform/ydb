LIBRARY()

SRCS(
    yql_structured_token.cpp
    yql_structured_token.h
    yql_token_builder.cpp
    yql_token_builder.h
)

PEERDIR(
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
