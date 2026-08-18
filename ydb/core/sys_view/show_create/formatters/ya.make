LIBRARY()

SRCS(
    create_external_data_source_formatter.cpp
    create_external_table_formatter.cpp
    create_table_formatter.cpp
    create_view_formatter.cpp
    formatters_common.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/tx/columnshard/engines/storage/indexes/helper
    ydb/core/formats/arrow/serializer
    ydb/core/protos
    ydb/core/tx/columnshard/engines/scheme/defaults/protos
    ydb/core/tx/sequenceproxy
    ydb/core/ydb_convert
    ydb/public/api/protos
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/types
    yql/essentials/ast
    yql/essentials/public/issue
    yql/essentials/sql/settings
    yql/essentials/sql/v1/translation
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
