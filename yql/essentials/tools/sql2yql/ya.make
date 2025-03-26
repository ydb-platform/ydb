IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    contrib/libs/antlr3_cpp_runtime
    library/cpp/getopt
    library/cpp/testing/unittest
    yql/essentials/parser/lexer_common
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/pg
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

ADDINCL(
    GLOBAL contrib/libs/antlr3_cpp_runtime/include
)

SRCS(
    sql2yql.cpp
)

END()

ENDIF()

