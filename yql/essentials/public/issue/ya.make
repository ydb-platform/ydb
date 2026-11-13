LIBRARY()

SRCS(
    yql_issue.cpp
    yql_issue_message.cpp
    yql_issue_manager.cpp
    yql_issue_utils.cpp
    yql_warning.cpp
    yql_issue_id.cpp
    yql_issue_message_impl.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/colorizer
    library/cpp/resource
    library/cpp/containers/cow_string
    yql/essentials/public/issue/protos
    yql/essentials/public/utf8
    yql/essentials/utils
)

GENERATE_ENUM_SERIALIZATION(yql_warning.h)

END()

RECURSE_FOR_TESTS(
    ut
)
