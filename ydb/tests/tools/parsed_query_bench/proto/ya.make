LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
)

SRCS(
    antlr_ast.proto
    antlr_cst.proto
    execute_query.proto
    logical_plan.proto
    main_stages.proto
    native_query.proto
    parsed_query.proto
    syntax_script.proto
    yql_parser_metadata.proto
)

END()
