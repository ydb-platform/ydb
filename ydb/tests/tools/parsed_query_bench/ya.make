PROGRAM(parsed_query_legacy_bench)

SRCS(
    engine.cpp
    kql_codec.cpp
    main.cpp
    syntax.cpp
)

SET(GRAMMAR_MANIFEST
    ${ARCADIA_ROOT}/yql/essentials/sql/v1/SQLv1Antlr4.g.in
    ${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/gen/v1_antlr4/ya.make
    ${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/ya.make
    ${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg.in
    ${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg.in
    ${ARCADIA_BUILD_ROOT}/yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.proto
)
RUN_PYTHON3(
    grammar_fingerprint.py
    ${BINDIR}/grammar_fingerprint.h ${GRAMMAR_MANIFEST}
    IN ${GRAMMAR_MANIFEST}
    OUT_NOAUTO grammar_fingerprint.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/json
    ydb/core/kqp/common/simple
    ydb/core/kqp/executer_actor
    ydb/core/kqp/gateway
    ydb/core/kqp/host
    ydb/core/kqp/node_service
    ydb/core/kqp/opt
    ydb/core/kqp/provider
    ydb/core/kqp/session_actor
    ydb/core/kqp/tracing
    ydb/core/scheme
    ydb/core/testlib/basics/core
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/tx/columnshard/data_sharing/manager
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
    ydb/core/tx/columnshard/engines/storage/indexes/max
    ydb/core/tx/columnshard/engines/storage/indexes/min_max
    ydb/core/tx/time_cast
    ydb/library/yql/providers/s3/provider
    ydb/public/api/protos
    ydb/tests/tools/parsed_query_bench/proto
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/services/mounts
    yql/essentials/parser/lexer_common
    yql/essentials/providers/common/arrow_resolve
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/translation
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

RESOURCE(
    ydb/core/kqp/kqp_default_settings.txt kqp_default_settings.txt
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
