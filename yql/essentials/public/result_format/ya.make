LIBRARY()

SRCS(
    yql_result_format_response.cpp
    yql_result_format_type.cpp
    yql_result_format_data.cpp
    yql_codec_results.cpp
    yql_restricted_yson.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    library/cpp/string_utils/base64
    yql/essentials/public/issue
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)

