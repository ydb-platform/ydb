LIBRARY()

SRCS(
    utf8.cpp
    yql_issue.cpp
    yql_issue_message.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/colorizer
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
