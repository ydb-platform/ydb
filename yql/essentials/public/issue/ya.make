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
    yql/essentials/public/issue/protos
    yql/essentials/utils
)

GENERATE_ENUM_SERIALIZATION(yql_warning.h)

END()

RECURSE_FOR_TESTS(
    ut
)
