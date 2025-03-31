PROGRAM()

SRCS(
    sql_functions_dump.cpp
)

PEERDIR(
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/utils/backtrace
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/stub
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)
