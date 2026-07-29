PROGRAM()

PEERDIR(
    library/cpp/getopt
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/accessor/common
    ydb/core/formats/arrow/serializer
    ydb/library/formats/arrow/protos
    ydb/services/metadata/abstract
    library/cpp/json
    yql/essentials/core
    yql/essentials/parser/lexer_common
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/translation
    yql/essentials/types/binary_json
)

YQL_LAST_ABI_VERSION()

NO_COMPILER_WARNINGS()

SRCS(
    main.cpp
)

END()
