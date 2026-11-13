G_BENCHMARK()

YQL_CURRENT_ABI_VERSION()

SRCS(
    benchmark.cpp
)

PEERDIR(
    yql/essentials/tools/purebench/gbenchmark
)

DATA(
    arcadia/yql/essentials/tests/sql_benchmarks/smoke/suites
)

SIZE(MEDIUM)

END()
