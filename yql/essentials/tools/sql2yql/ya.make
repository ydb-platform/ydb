PROGRAM()

PEERDIR(
    contrib/libs/antlr3_cpp_runtime
    library/cpp/getopt
    library/cpp/testing/unittest
    contrib/ydb/library/yql/parser/lexer_common
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/sql/v1/format
)

ADDINCL(
    GLOBAL contrib/libs/antlr3_cpp_runtime/include
)

SRCS(
    sql2yql.cpp
)

END()
