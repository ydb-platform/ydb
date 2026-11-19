LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
)

SRCS(
    execute_query.proto
    kql_query_list.proto
    parsed_query.proto
)

END()
