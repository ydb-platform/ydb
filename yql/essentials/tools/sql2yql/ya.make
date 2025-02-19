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
)

ADDINCL(
    GLOBAL contrib/libs/antlr3_cpp_runtime/include
)

SRCS(
    sql2yql.cpp
)

END()
