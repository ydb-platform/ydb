G_BENCHMARK()

SRCS(
    benchmark.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/pure_ast
)

RESOURCE(
    yql/essentials/tests/sql/suites/select_yql/minimal.yql select-yql-minimal.yql
    yql/essentials/tests/sql/suites/select_yql_tpch/q15.yql yql-tpch-q15.yql
    yql/essentials/tests/sql/suites/select_yql_tpcds/q47.yql yql-tpcds-q47.yql
)

END()
