LIBRARY()

SRCS(
    util.cpp
    view_utils.cpp
)

PEERDIR(
    library/cpp/protobuf/util
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/status
    yql/essentials/parser/proto_ast/gen/v1
    yql/essentials/parser/proto_ast/gen/v1_proto_split
    yql/essentials/sql/settings
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/proto_parser
)

END()
