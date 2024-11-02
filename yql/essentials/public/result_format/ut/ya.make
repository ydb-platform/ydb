UNITTEST_FOR(yql/essentials/public/result_format)

SRCS(
    yql_result_format_response_ut.cpp
    yql_result_format_type_ut.cpp
    yql_result_format_data_ut.cpp
    yql_restricted_yson_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/string_utils/base64
)

END()
