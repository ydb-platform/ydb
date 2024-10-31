LIBRARY()

SRCS(
    yql_issue.cpp
    yql_issue_message.cpp
    yql_issue_manager.cpp
    yql_issue_utils.cpp
    yql_warning.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/colorizer
    library/cpp/resource
    contrib/ydb/public/api/protos
    yql/essentials/public/issue/protos
    contrib/ydb/library/yql/utils
)

GENERATE_ENUM_SERIALIZATION(yql_warning.h)

END()

RECURSE_FOR_TESTS(
    ut
)
