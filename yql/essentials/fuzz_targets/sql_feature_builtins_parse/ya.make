FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
