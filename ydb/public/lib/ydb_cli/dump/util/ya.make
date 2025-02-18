LIBRARY()

SRCS(
    query_utils.cpp
    util.cpp
    view_utils.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/status
    yql/essentials/parser/proto_ast/gen/v1
    yql/essentials/parser/proto_ast/gen/v1_proto_split
    yql/essentials/sql/settings
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/proto_parser
    library/cpp/protobuf/util
)

END()
