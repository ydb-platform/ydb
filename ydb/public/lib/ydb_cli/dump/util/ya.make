LIBRARY()

SRCS(
    util.cpp
    view_utils.cpp
)

PEERDIR(
    library/cpp/protobuf/util
    ydb/library/yql/parser/proto_ast/gen/v1
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
    ydb/library/yql/sql/settings
    ydb/library/yql/sql/v1/format
    ydb/library/yql/sql/v1/proto_parser
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/status
)

END()
