PROGRAM()

PEERDIR(
    contrib/libs/antlr3_cpp_runtime
    library/cpp/getopt
    library/cpp/testing/unittest
    ydb/library/yql/parser/lexer_common
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql
    ydb/library/yql/sql/pg
    ydb/library/yql/sql/v1/format
)

ADDINCL(
    GLOBAL contrib/libs/antlr3_cpp_runtime/include
)

SRCS(
    sql2yql.cpp
)

END()
