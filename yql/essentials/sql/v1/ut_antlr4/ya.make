UNITTEST_FOR(yql/essentials/sql/v1)

SRCS(
    sql_ut_antlr4.cpp
    sql_match_recognize_ut.cpp
)

PEERDIR(
    library/cpp/regex/pcre
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/sql_types
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi

    #FIXME - switch sql_match_recognize_ut.cpp to antlr4
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
