LIBRARY()

PEERDIR(
    ydb/library/yql/utils

    ydb/library/yql/parser/proto_ast
    ydb/library/yql/parser/proto_ast/collect_issues
    ydb/library/yql/parser/proto_ast/gen/v1
    ydb/library/yql/parser/proto_ast/gen/v1_ansi
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
)

SRCS(
    proto_parser.cpp
)

END()
